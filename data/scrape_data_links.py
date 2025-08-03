from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import pandas as pd
from libs.write_to_s3 import WriteToS3
from my_secrets import aws, host


class ScrapeDataLinks:
    def __init__(self):
        self.host = host
        self.dataset = []

    def read_links(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(self.host)
        expand_all_button = driver.find_element(By.CLASS_NAME, "faq-expand-all")
        expand_all_button.click()

        freq_ans_divs = driver.find_elements(By.CLASS_NAME, "faq-answers")
        freq_qtns_divs = driver.find_elements(By.CLASS_NAME, "faq-questions")

        n = 0
        for freq_ans_div, freq_qtns_div in zip(freq_ans_divs, freq_qtns_divs):
            n += 1
            year = freq_qtns_div.find_element(By.TAG_NAME, "p").text
            table = freq_ans_div.find_element(By.TAG_NAME, "table")
            cells = table.find_elements(By.TAG_NAME, "td")
            for cell in cells:
                months = cell.find_elements(By.TAG_NAME, "strong")
                month_data_lists = cell.find_elements(By.TAG_NAME, "ul")
                for month, data_list in zip(months, month_data_lists):
                    month_link_list = data_list.find_elements(By.TAG_NAME, "li")
                    link_list = [
                        {
                            "year": year,
                            "month": month.text,
                            "title": link_tag.find_element(By.TAG_NAME, "a").text,
                            "link": link_tag.find_element(
                                By.TAG_NAME, "a"
                            ).get_property("href"),
                        }
                        for link_tag in month_link_list
                    ]
                    for obj in link_list:
                        self.dataset.append(obj)
        self.df = pd.DataFrame(self.dataset)

    def run(self):
        self.read_links()
        # self.df.to_csv("data/data.csv", index=False)
        s3_write = WriteToS3(aws["bucket"], "links", "links_to_data.csv")
        s3_write.write(self.df)


if __name__ == "__main__":
    scraper = ScrapeDataLinks()
    scraper.run()
