import boto3, os, io, pandas as pd

S3_BUCKET = os.getenv("S3_RAW_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX", "").strip("/")

def read_csv_from_s3(key: str) -> pd.DataFrame:
    """Read one CSV from S3 and return as DataFrame"""
    s3 = boto3.client("s3")
    prefix = f"{S3_PREFIX}/" if S3_PREFIX else ""
    obj = s3.get_object(Bucket=S3_BUCKET, Key=f"{prefix}{key}")
    return pd.read_csv(io.BytesIO(obj["Body"].read()))
