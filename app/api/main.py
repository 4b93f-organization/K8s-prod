import json
import uuid
import boto3
import os
from fastapi import FastAPI, UploadFile, File
from botocore.exceptions import ClientError

app = FastAPI()

S3_BUCKET = os.getenv("S3_BUCKET", "log-processing")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def get_s3():
    return boto3.client("s3", endpoint_url=AWS_ENDPOINT_URL, region_name=AWS_REGION)

def get_sqs():
    return boto3.client("sqs", endpoint_url=AWS_ENDPOINT_URL, region_name=AWS_REGION)


@app.get("/health")
def health():
    return {"status": "ok", "version": "0.1.0"}


@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    job_id = str(uuid.uuid4())
    s3_key = f"uploads/{job_id}/{file.filename}"

    s3 = get_s3()
    s3.upload_fileobj(file.file, S3_BUCKET, s3_key)

    sqs = get_sqs()
    sqs.send_message(
        QueueUrl=SQS_QUEUE_URL,
        MessageBody=json.dumps({"job_id": job_id, "s3_key": s3_key}),
    )

    return {"job_id": job_id, "status": "queued"}


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    s3 = get_s3()
    result_key = f"results/{job_id}/summary.json"

    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=result_key)
        return json.loads(obj["Body"].read())
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return {"job_id": job_id, "status": "pending"}
        raise