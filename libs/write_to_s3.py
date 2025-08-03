import boto3
from my_secrets import aws
from io import StringIO


class WriteToS3:
    def __init__(self, bucket, prefix, object_name):
        self.bucket = bucket
        self.prefix = prefix
        self.object_name = object_name
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=aws["ACCESS_KEY"],
            aws_secret_access_key=aws["SECRET_KEY"],
        )

    def write(self, object):
        csv_buffer = StringIO()
        object.to_csv(csv_buffer, index=False)
        self.s3.put_object(
            Bucket=self.bucket,
            Key=f"{self.prefix}/{self.object_name}",
            Body=csv_buffer.getvalue(),
        )
