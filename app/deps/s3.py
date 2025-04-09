import os

import boto3
from botocore.config import Config


def _get_env_or_raise(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(f"Missing required environment variable: {key}")
    return value


def s3_upload(local_path: str, s3_key: str):
    """
    Upload a local file to S3 (force overwrite).

    Env vars required:
      - S3_ACCESS_KEY
      - S3_SECRET_KEY
      - S3_BUCKET
      - S3_ENDPOINT
    """
    # Get env variables
    access_key = _get_env_or_raise("S3_ACCESS_KEY")
    secret_key = _get_env_or_raise("S3_SECRET_KEY")
    bucket_name = _get_env_or_raise("S3_BUCKET")
    # TODO
    # endpoint = _get_env_or_raise("S3_ENDPOINT")
    endpoint = "localhost:9000"

    if not endpoint.startswith("http"):
        endpoint = f"http://{endpoint}"

    # Create boto3 S3 client
    s3 = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint,
        config=Config(signature_version="s3v4"),
        verify=False
    )

    # Upload the file
    s3.upload_file(
        Filename=local_path,
        Bucket=bucket_name,
        Key=s3_key
    )
