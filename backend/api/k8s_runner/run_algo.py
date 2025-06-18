import os
import boto3
import pandas as pd
import numpy as np
import traceback
import json
from sqlalchemy import create_engine
from sub_test_algo import get_asset_data
import ta
import scipy
import statistics
import statsmodels.api as sm
import statsmodels

def main():
    try:
        print("starting algo runner inside container...")

        # Load ENV
        s3_key = os.environ["S3_KEY"]
        func_name = os.environ["FUNC_NAME"]
        table_name = os.environ["TABLE_NAME"]
        result_key = os.environ["RESULT_KEY"]
        db_password = os.environ["DB_PASSWORD"]
        db_uri = os.environ["DB_URI"]
        bucket = os.environ["AWS_STORAGE_BUCKET_NAME"]
        aws_region = os.environ["AWS_REGION"]
        aws_access_key = os.environ["AWS_ACCESS_KEY_ID"]
        aws_secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]

        # Download user Python algo code
        print(f"downloading {s3_key} from bucket {bucket}")
        s3 = boto3.client("s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region
        )
        obj = s3.get_object(Bucket=bucket, Key=s3_key)
        code = obj["Body"].read().decode("utf-8")

        # Connect to RDS
        db_url = f"postgresql://postgres:{db_password}@{db_uri}:5432/stock-data"
        engine = create_engine(db_url)

        print(f"fetching data from table: {table_name}")
        df = get_asset_data(table_name, engine)

        # Setup exec() env
        local_ns = {}
        global_env = {
            "pd": pd,
            "np": np,
            "ta": ta,
            "scipy": scipy,
            "statistics": statistics,
            "sm": sm,
            "statsmodels": statsmodels,
            "__builtins__": __builtins__,
        }

        exec(code, global_env, local_ns)
        func = local_ns.get(func_name)
        if not callable(func):
            raise ValueError(f"{func_name} is not a callable function")

        print("⚙️ Running user-defined function...")
        result = func(df)

        # Validate result
        if not isinstance(result, (list, np.ndarray)):
            raise ValueError("Result must be a list or numpy array")
        if len(result) != len(df):
            raise ValueError("Return length does not match input rows")
        allowed = {"Buy", "Sell", "Hold"}
        if not all(str(x) in allowed for x in result):
            raise ValueError("Invalid return value in result list")

        print("uploading result to S3...")
        s3.put_object(
            Bucket=bucket,
            Key=result_key,
            Body=json.dumps(list(result))  # Ensure array is JSON serializable
        )

        print("Job finished successfully.")

    except Exception as e:
        print("error:", str(e))
        traceback.print_exc()

if __name__ == "__main__":
    main()
