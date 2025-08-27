import os, pandas as pd, sqlalchemy as sa, mlflow, numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

WAREHOUSE_URL = os.getenv("WAREHOUSE_URL", "postgresql+psycopg2://wh:wh@warehouse:5432/gold")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5001")

def train_and_log(**_):
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment("mlops-demo")

    df = pd.read_sql("select * from features_gold", sa.create_engine(WAREHOUSE_URL))
    X = df.select_dtypes(include="number")
    y = df["label"].astype(int) if "label" in df.columns else (np.tanh(X.sum(axis=1)) > 0).astype(int)

    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.2, random_state=42)

    with mlflow.start_run():
        model = LogisticRegression(max_iter=300).fit(Xtr, ytr)
        auc = roc_auc_score(yte, model.predict_proba(Xte)[:,1])
        mlflow.log_metric("auc", float(auc))
        mlflow.sklearn.log_model(model, artifact_path="model")
