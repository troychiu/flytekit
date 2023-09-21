
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_smithy_http::byte_stream::{ByteStream, Length};
use futures::future::join_all;
use pyo3::exceptions::{self};
use pyo3::prelude::*;
use aws_sdk_s3::{config::Region, Client, Error};
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;

const CHUNK_SIZE: u64 = 1024 * 1024 * 50;
const READCHUNK: u64 = 1024 * 1024 * 50;
#[allow(dead_code)]
fn print_type_of<T>(_: &T) {
    println!("{}", std::any::type_name::<T>())
}

#[pyclass]
pub struct S3FileSystem {
    #[pyo3(get, set)]
    pub endpoint: String,

    s3_client: aws_sdk_s3::Client,
}

fn build_client(endpoint: &str) -> aws_sdk_s3::Client {
    let region = Region::new("us-west-2");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let conf = rt.block_on(async { aws_config::load_from_env().await });
    let s3_conf = match endpoint.is_empty() {
        true => aws_sdk_s3::config::Builder::from(&conf).region(region).build(),
        false => aws_sdk_s3::config::Builder::from(&conf)
            .endpoint_url(endpoint)
            .region(region)
            .force_path_style(true)
            .build(),
    };

    Client::from_conf(s3_conf)
}

// Write contents of ByteStream into destination buffer.
async fn drain_stream(mut s: ByteStream, dest: &mut [u8]) -> Result<usize, Error> {
    let mut offset = 0;
    while let Ok(Some(bytes)) = s.try_next().await {
        let span = offset..offset + bytes.len();
        dest[span].clone_from_slice(&bytes);
        offset += bytes.len();
    }
    Ok(offset)
}

// async fn write_buffer(mut body: ByteStream, file: &mut File) -> Result<(), Error> {
//     while let Ok(Some(bytes)) = body.try_next().await {
//         let _ = file.write(&bytes);
//     }
//     Ok(())
// }

impl S3FileSystem {
    fn get_client(&self) -> &aws_sdk_s3::Client {
        &self.s3_client
    }
}

#[pymethods]
impl S3FileSystem {
    #[new]
    pub fn new(endpoint: String) -> S3FileSystem {
        let c = build_client(&endpoint);
        S3FileSystem {
            endpoint: endpoint,
            s3_client: c,
        }
    }

    pub fn put_file(&self, lpath: String, bucket: String, key: String) -> PyResult<()> {
        let client = self.get_client();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let file_size = std::fs::metadata(&lpath).unwrap().len();
            let mut chunk_count = (file_size / CHUNK_SIZE) + 1;
            let mut size_of_last_chunk = file_size % CHUNK_SIZE;
            if size_of_last_chunk == 0 {
                size_of_last_chunk = CHUNK_SIZE;
                chunk_count -= 1;
            }

            let multipart_upload_res: CreateMultipartUploadOutput = client
                .create_multipart_upload()
                .bucket(&bucket)
                .key(&key)
                .send()
                .await
                .unwrap();
            let upload_id = multipart_upload_res.upload_id.unwrap();

            // Create futures for each chunk upload
            let upload_futures: Vec<_> = (0..chunk_count).map(|chunk_index| {
                let client = &client;
                let lpath = &lpath;
                let bucket = &bucket;
                let key = &key;
                let upload_id = &upload_id;

                async move {
                    let this_chunk = if chunk_count - 1 == chunk_index {
                        size_of_last_chunk
                    } else {
                        CHUNK_SIZE
                    };
                    let stream = ByteStream::read_from()
                        .path(lpath)
                        .offset(chunk_index * CHUNK_SIZE)
                        .length(Length::Exact(this_chunk))
                        .build()
                        .await
                        .unwrap();

                    // Chunk index needs to start at 0, but part numbers start at 1.
                    let part_number = (chunk_index as i32) + 1;
                    let upload_part_res = client.upload_part()
                        .key(key)
                        .bucket(bucket)
                        .upload_id(upload_id)
                        .body(stream)
                        .part_number(part_number)
                        .send()
                        .await
                        .unwrap();

                    CompletedPart::builder()
                        .e_tag(upload_part_res.e_tag.unwrap_or_default())
                        .part_number(part_number)
                        .build()
                }
            }).collect();

            // Join all the futures to upload the chunks concurrently.
            let upload_parts = join_all(upload_futures).await;

            let completed_multipart_upload: CompletedMultipartUpload = CompletedMultipartUpload::builder()
                .set_parts(Some(upload_parts))
                .build();

            let _complete_multipart_upload_res = client
                .complete_multipart_upload()
                .bucket(&bucket)
                .key(&key)
                .multipart_upload(completed_multipart_upload)
                .upload_id(&upload_id)
                .send()
                .await
                .unwrap();
        });
        Ok(())
    }

    pub fn get_file(&self, lpath: String, bucket: String, key: String) -> PyResult<()> {
        let client = self.get_client();
        let rt = tokio::runtime::Runtime::new().unwrap();
    
        rt.block_on(async {
            // 1. Determine the size of the object in S3.
            let head_object_output = client.head_object()
                .bucket(&bucket)
                .key(&key)
                .send()
                .await
                .unwrap();
            let file_size = head_object_output.content_length() as u64;
    
            // 2. Split the object's byte range into chunks.
            let mut chunk_count = file_size / CHUNK_SIZE;
            if file_size % CHUNK_SIZE > 0 {
                chunk_count += 1;
            }
    
            // 3. Download each chunk concurrently.
            let download_futures: Vec<_> = (0..chunk_count).map(|chunk_index| {
                let client = &client;
                let bucket = &bucket;
                let key = &key;
            
                async move {
                    let start_byte = chunk_index * CHUNK_SIZE;
                    let end_byte = std::cmp::min(start_byte + CHUNK_SIZE, file_size) - 1;
                    
                    let get_object_output = client.get_object()
                        .bucket(bucket)
                        .key(key)
                        .range(&format!("bytes={}-{}", start_byte, end_byte))
                        .send()
                        .await
                        .unwrap();
            
                    let chunk_size = (end_byte - start_byte + 1) as usize;
                    let mut buffer = vec![0; chunk_size];
                    let bytes_read = drain_stream(get_object_output.body, &mut buffer).await.unwrap();
                    buffer.truncate(bytes_read); // Ensure buffer only has valid bytes
            
                    (chunk_index, buffer)
                }
            }).collect();
    
            let mut chunks = join_all(download_futures).await;
            chunks.sort_by(|(index1, _), (index2, _)| index1.partial_cmp(index2).unwrap());
            let mut file = tokio::fs::File::create(lpath).await.unwrap();
            for (_, buffer) in chunks {
                file.write_all(&buffer).await.unwrap();
            }
        });
    
        Ok(())
    }

    // pub fn put_file(&self, lpath: String, bucket: String, key: String) -> PyResult<()> {
    //     let client = self.get_client();
    //     let rt = tokio::runtime::Runtime::new().unwrap();
    //     let _ = rt.block_on(async {
    //         let body = ByteStream::from_path(Path::new(&lpath)).await;
    //         let _ = match client
    //             .put_object()
    //             .bucket(bucket)
    //             .key(key)
    //             .body(body.unwrap())
    //             .send()
    //             .await
    //             {
    //                 Ok(r) => r,
    //                 Err(e) => return Err(PyIOError::new_err(e.to_string())),
    //             };
    //         Ok(())
    //     });
    //     Ok(())
    // }

    // pub fn get_file(&self, lpath: String, bucket: String, key: String) -> PyResult<()> {
    //     let client = self.get_client();
    //     let mut file = File::create(lpath.clone())?;
        
        
    //     let rt = tokio::runtime::Runtime::new().unwrap();
    //     let mut read_reqs = vec![];
    //     let _ = rt.block_on(async {
    //         let head_result = match client
    //             .head_object()
    //             .bucket(&bucket)
    //             .key(&key)
    //             .send()
    //             .await {
    //                 Ok(r) => r,
    //                 Err(e) => return Err(PyIOError::new_err(e.to_string())),
    //             };
    //         let obj_size = head_result.content_length() as usize;

            
    //         let mut read_offset = 0;
    //         while read_offset < obj_size {
    //             let read_upper = std::cmp::min(obj_size, read_offset + READCHUNK);
    //             let byte_range = format!("bytes={}-{}", read_offset, read_upper - 1);
    //             let object = match client
    //                 .get_object()
    //                 .bucket(&bucket)
    //                 .key(&key)
    //                 .range(byte_range)
    //                 .send()
    //                 .await
    //                 {
    //                     Ok(r) => r,
    //                     Err(e) => return Err(PyIOError::new_err(e.to_string())),
    //                 };
    //             read_reqs.push(write_buffer(object.body, &mut file));
    //         }
    //         let _results = try_join_all(read_reqs).await.unwrap();
    //         Ok(())
    //     });
    //     Ok(())
    // }


    // pub fn get_objects(&self, py: Python, paths: Vec<String>) -> PyResult<PyObject> {
    //     let pathpairs: Vec<(String, String)> = paths.iter().map(path_to_bucketprefix).collect();

    //     let client = self.get_client();
    //     let rt = tokio::runtime::Runtime::new().unwrap();

    //     let mut pybuf_list = Vec::new();
    //     for _ in &pathpairs {
    //         pybuf_list.push(PyByteArray::new(py, &[]));
    //     }

    //     let return_buf = rt.block_on(async {
    //         let mut head_reqs = vec![];
    //         for (bucket, key) in &pathpairs {
    //             head_reqs.push(client.head_object().bucket(bucket).key(key).send());
    //         }
    //         let head_results = match try_join_all(head_reqs).await {
    //             Ok(r) => r,
    //             Err(e) => return Err(PyIOError::new_err(e.to_string())),
    //         };
    //         let obj_sizes: Vec<usize> = head_results.iter().map(|x| x.content_length() as usize).collect();

    //         for (p, o) in pybuf_list.iter_mut().zip(obj_sizes) {
    //             p.resize(o)?;
    //         }

    //         let mut read_reqs = vec![];

    //         for (pybuf, (bucket, key)) in pybuf_list.iter_mut().zip(&pathpairs) {
    //             let obj_size = pybuf.len();
    //             let landing_buf = unsafe { pybuf.as_bytes_mut() };
    //             let mut landing_slices: Vec<&mut [u8]> = landing_buf.chunks_mut(READCHUNK).collect();

    //             let mut read_offset = 0;
    //             while read_offset < obj_size {
    //                 let read_upper = std::cmp::min(obj_size, read_offset + READCHUNK);
    //                 let byte_range = format!("bytes={}-{}", read_offset, read_upper - 1);

    //                 let resp = match client
    //                     .get_object()
    //                     .bucket(bucket)
    //                     .key(key)
    //                     .range(byte_range)
    //                     .send()
    //                     .await
    //                 {
    //                     Ok(r) => r,
    //                     Err(e) => return Err(PyIOError::new_err(e.to_string())),
    //                 };

    //                 read_reqs.push(drain_stream(resp.body, landing_slices.remove(0)));

    //                 read_offset += READCHUNK;
    //             }
    //         }
    //         let _results = try_join_all(read_reqs).await.unwrap();

    //         let pybufs: &PyList = PyList::new(py, pybuf_list);
    //         Ok(pybufs)
    //     });

    //     match return_buf {
    //         Ok(b) => Ok(b.into()),
    //         Err(e) => Err(e),
    //     }
    // }
}

fn to_py_err<E: std::fmt::Display>(err: E) -> PyErr {
    PyErr::new::<exceptions::PyRuntimeError, _>(format!("{}", err))
}

// #[pyfunction]
// fn upload_to_s3(
//     file_path: &str,
//     bucket: &str,
//     key: &str,
//     aws_access_key_id: &str,
//     aws_secret_access_key: &str,
//     endpoint_url: &str,
//     token: Option<String>,  // Note: This is an Option in case the token might not always be provided
// ) -> PyResult<()> {
//     let creds = AwsCredentials::new(aws_access_key_id, aws_secret_access_key, token, None);
//     let region = Region::Custom {
//         name: "us-east-2".to_string(), // This is an arbitrary value; you can set it to a more meaningful name if needed
//         endpoint: endpoint_url.to_string(),
//     };
//     let client = HttpClient::new().map_err(to_py_err)?;
//     let s3_client = S3Client::new_with(client, StaticProvider::from(creds), region);

//     // Put object
//     let mut f = File::open(file_path).map_err(to_py_err)?;
//     let mut data = Vec::new();
//     f.read_to_end(&mut data).map_err(to_py_err)?;

//     let put_req = PutObjectRequest {
//         bucket: bucket.to_string(),
//         key: key.to_string(),
//         body: Some(data.into()),
//         ..Default::default()
//     };

//     let runtime = tokio::runtime::Runtime::new().map_err(to_py_err)?;
//     runtime.block_on(s3_client.put_object(put_req)).map_err(to_py_err)?;

//     Ok(())
// }

// #[pyfunction]
// fn download_from_s3(file_path: &str, bucket: &str, key: &str) -> PyResult<()> {
//     let s3_client = S3Client::new(Region::UsWest1); // Adjust the region as needed

//     // Get object
//     let get_req = GetObjectRequest {
//         bucket: bucket.to_string(),
//         key: key.to_string(),
//         ..Default::default()
//     };

//     let runtime = tokio::runtime::Runtime::new().map_err(to_py_err)?;
//     let output = runtime.block_on(s3_client.get_object(get_req)).map_err(to_py_err)?;

//     // Write the output to the provided file path
//     if let Some(stream) = output.body {
//         // We use block_on again for the asynchronous File operations
//         let mut file = runtime.block_on(tokio::fs::File::create(file_path)).map_err(to_py_err)?;

//         runtime.block_on(async {
//             let mut async_reader = stream.into_async_read();
//             tokio::io::copy(&mut async_reader, &mut file).await
//         }).map_err(to_py_err)?;
//     } else {
//         return Err(PyErr::new::<exceptions::PyValueError, _>("No content found in S3 object"));
//     }

//     Ok(())
// }

#[pymodule]
fn rustfs(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<S3FileSystem>()?;
    // m.add_function(wrap_pyfunction!(upload_to_s3, m)?)?;
    // m.add_function(wrap_pyfunction!(download_from_s3, m)?)?;
    Ok(())
}