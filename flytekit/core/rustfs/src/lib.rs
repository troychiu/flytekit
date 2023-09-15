use pyo3::exceptions;
use pyo3::prelude::*;
use rusoto_core::credential::{AwsCredentials, StaticProvider};
use rusoto_core::{HttpClient, Region};
use rusoto_s3::{PutObjectRequest, S3, S3Client};
use std::fs::File;
use std::io::Read;

fn to_py_err<E: std::fmt::Display>(err: E) -> PyErr {
    PyErr::new::<exceptions::PyRuntimeError, _>(format!("{}", err))
}

#[pyfunction]
fn upload_to_s3(
    file_path: &str,
    bucket: &str,
    key: &str,
    aws_access_key_id: &str,
    aws_secret_access_key: &str,
    endpoint_url: &str,
    token: Option<String>,  // Note: This is an Option in case the token might not always be provided
) -> PyResult<()> {
    let creds = AwsCredentials::new(aws_access_key_id, aws_secret_access_key, token, None);
    let region = Region::Custom {
        name: "us-east-2".to_string(), // This is an arbitrary value; you can set it to a more meaningful name if needed
        endpoint: endpoint_url.to_string(),
    };
    let client = HttpClient::new().map_err(to_py_err)?;
    let s3_client = S3Client::new_with(client, StaticProvider::from(creds), region);

    // Put object
    let mut f = File::open(file_path).map_err(to_py_err)?;
    let mut data = Vec::new();
    f.read_to_end(&mut data).map_err(to_py_err)?;

    let put_req = PutObjectRequest {
        bucket: bucket.to_string(),
        key: key.to_string(),
        body: Some(data.into()),
        ..Default::default()
    };

    let runtime = tokio::runtime::Runtime::new().map_err(to_py_err)?;
    runtime.block_on(s3_client.put_object(put_req)).map_err(to_py_err)?;

    Ok(())
}

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
    m.add_function(wrap_pyfunction!(upload_to_s3, m)?)?;
    // m.add_function(wrap_pyfunction!(download_from_s3, m)?)?;
    Ok(())
}