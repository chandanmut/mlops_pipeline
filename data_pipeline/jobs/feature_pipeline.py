import os, io, boto3, pandas as pd, sqlalchemy as sa

S3_BUCKET = os.getenv("S3_RAW_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX", "").strip("/")       # e.g., "mlops-project" or ""
WAREHOUSE_URL = os.getenv("WAREHOUSE_URL", "postgresql+psycopg2://wh:wh@localhost:5432/gold")

def _raw_prefix():
    return f"{S3_PREFIX + '/' if S3_PREFIX else ''}raw/"

def extract_from_s3(**_):
    s3 = boto3.client("s3")
    prefix = _raw_prefix()
    resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    keys = [o["Key"] for o in (resp.get("Contents") or []) if o["Key"].lower().endswith(".csv")]
    if not keys:
        raise ValueError(f"No CSVs found under s3://{S3_BUCKET}/{prefix}")
    frames = []
    for k in keys:
        body = s3.get_object(Bucket=S3_BUCKET, Key=k)["Body"].read()
        frames.append(pd.read_csv(io.BytesIO(body)))
    pd.concat(frames, ignore_index=True).to_parquet("/tmp/raw.parquet", index=False)

def transform(**_):
    df = pd.read_parquet("/tmp/raw.parquet")
    for col in df.select_dtypes(include="number").columns:
        mu, sigma = df[col].mean(), df[col].std() or 1.0
        df[f"{col}_z"] = (df[col] - mu) / sigma
    df.dropna(inplace=True)
    df.to_parquet("/tmp/gold.parquet", index=False)

def load_to_warehouse(**_):
    df = pd.read_parquet("/tmp/gold.parquet")
    engine = sa.create_engine(WAREHOUSE_URL)
    df.to_sql("features_gold", engine, if_exists="replace", index=False)
