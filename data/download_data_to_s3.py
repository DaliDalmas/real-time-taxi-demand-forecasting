from libs.manage_s3 import Manages3
from my_secrets import aws
import requests


class DownloadDataToS3:
    def __init__(self):
        self.url_for_list_of_links = f"s3://{aws['bucket']}/links/links_to_data.csv"
        self.link_list = None

    def download(self):
        s3_manager = Manages3(bucket=aws["bucket"])
        # read list of parquet links
        self.link_list = s3_manager.read(url=self.url_for_list_of_links)
        for row in self.link_list.to_dict(orient="records"):
            link = row["link"]
            object_name = link.split("/")[-1]
            year = row["year"]
            month = row["month"]
            response = requests.get(link)
            s3_manager.write(
                object_name=object_name,
                object=response.content,
                prefix=f"trip-data/{year}/{month}",
                object_type="parquet",
            )
            print(year, month, link)

    def run(self):
        self.download()
        print(self.link_list)


if __name__ == "__main__":
    DownloadDataToS3().run()
