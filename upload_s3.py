# upload_s3.py
import boto3, sys
s3 = boto3.client("s3")
if __name__ == "__main__":
    bucket = sys.argv[1]  # your-bucket
    local_file = sys.argv[2]  # sample_orders.csv
    key = sys.argv[3]       # input/sample_orders.csv
    s3.upload_file(local_file, bucket, key)
    print("uploaded", local_file, "to s3://%s/%s" % (bucket, key))
