import boto3
from my_secrets import aws
from io import StringIO
import pandas as pd


class Manages3:
    def __init__(self, bucket):
        self.bucket = bucket
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=aws["ACCESS_KEY"],
            aws_secret_access_key=aws["SECRET_KEY"],
        )

    def write(self, object: any, object_name: str, prefix: str, object_type: str, dataframe=False) -> None:
        if object_type=='csv':
            csv_buffer = StringIO()
            object.to_csv(csv_buffer, index=False)
            self.s3.put_object(
                Bucket=self.bucket,
                Key=f"{prefix}/{object_name}",
                Body=csv_buffer.getvalue(),
            )
        elif object_type=='parquet':
            if dataframe:
                pass
            else:
                self.s3.put_object(
                    Bucket=self.bucket,
                    Key=f"{prefix}/{object_name}",
                    Body=object,
                )   

    def read(self, url: str = None, urls: list = None) -> None:
        if isinstance(url, str) and not isinstance(urls, list):
            df = pd.read_csv(url)
        elif isinstance(urls, str) and not isinstance(url, list):
            pass
        else:
            raise ValueError(
                "`Key` and `keys` cannnot be both provided or both missing. Provide one."
            )
        return df
