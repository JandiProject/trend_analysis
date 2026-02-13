import boto3
import pandas as pd
from io import BytesIO
import dotenv
dotenv.load_dotenv("/Users/jeongdaegyun/airflow_dc/.env")

# --- Configuration ---
BUCKET_NAME = 'jandi-post-bucket'
OBJECT_KEY = 'blog-data/raw/dt=2026-02-09/네이버 D2_200712.parquet'

# Initialize an S3 client (ensure your AWS credentials are configured in your environment)
s3_client = boto3.client('s3')

# Get the S3 object
s3_response_object = s3_client.get_object(Bucket=BUCKET_NAME, Key=OBJECT_KEY)

# Read the file's body (stream of bytes)
df_bytes = s3_response_object['Body'].read()

# Read the bytes into a pandas DataFrame using BytesIO
df = pd.read_parquet(BytesIO(df_bytes))

# Display the first few rows of the DataFrame
print(df.columns.tolist())