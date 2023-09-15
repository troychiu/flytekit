import asyncio
from s3fs import S3FileSystem
from fsspec.callbacks import _DEFAULT_CALLBACK
from fsspec.utils import isfilelike
from rustfs import rustfs
import os

class RustS3FileSystem(S3FileSystem):
    """
    Want this to behave mostly just like the HTTP file system.
    """

    def __init__(self, **s3kwargs):
        super().__init__(**s3kwargs)


    # def get_file(self, rpath, lpath, callback=_DEFAULT_CALLBACK, outfile=None, **kwargs):
    #     print("Rust get file is called")
    #     bucket, key, version = self.split_path(rpath)
    #     rustfs.upload_to_s3(lpath, bucket, key)

    async def _put_file(self, lpath, rpath, callback=_DEFAULT_CALLBACK, **kwargs):
        print("Rust put_file is called")
        bucket, key, _ = self.split_path(rpath)
        await asyncio.to_thread(rustfs.upload_to_s3, lpath, bucket, key, self.key, self.secret, self.client_kwargs['endpoint_url'], self.token)
    
