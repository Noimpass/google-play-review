import logging
import warnings
import os
import json
import subprocess
from time import sleep
from typing import List

import tqdm
import pandas as pd
import translators as ts

from google_play_scraper import Sort, reviews, app

warnings.simplefilter(action='ignore', category=FutureWarning)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

file_handler = logging.FileHandler('app.log', mode='a')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.ERROR)
console_handler.setFormatter(formatter)

logging.addLevelName(55, "START")
logging.addLevelName(60, "SUCCESS")
logging.addLevelName(70, "SYSTEM")

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

logger.log(70, "==========Start==========")

class ParseReviews:
    def __init__(self, links: List[str], path: str):
        self.links = links
        self.lang = json.loads(open('lang.json').read())
        self.path = path

    def scrape(self):
        for link in self.links:
            name = link.split("=")[1]
            info = app(
                name,
                lang='ru', # defaults to 'en'
                country='ru', # defaults to 'us'
            )
            reviews_count = 0
            title = info["title"].split(" ")[0]
            logger.log(55,f"Parsing {title}")
            for j in range(1,6):
                vpn = subprocess.Popen("sudo wg-quick up", stdout=subprocess.PIPE)
                for i in range(len(self.lang["languages"])):
                    language = self.lang["languages"][i]["lang"]
                    country = self.lang["languages"][i]["country"]
                    number_of_zero_new_reviews = 0
                    result = []
                    new_result = None
                    continuation_token = None
                    while True:
                        try:
                            logger.info(f"Start collecting reviews for {country}-{language} with score {j}")
                            new_result, continuation_token = reviews(
                            name,
                            continuation_token=continuation_token,
                            lang=language, 
                            country=country, 
                            count=20000,
                            sort=Sort.MOST_RELEVANT, 
                            filter_score_with=j,
                            )
                            result.extend(new_result)
                            if not new_result:
                                number_of_zero_new_reviews += 1
                                sleep(30)

                            if len(result) >= 100000:
                                self.to_excel(result, title, j, language)
                                reviews_count += len(result)
                                result = []
                                number_of_zero_new_reviews = 0
                                break
                                
                            if number_of_zero_new_reviews == 10:
                                logger.info(f"No new reviews for {country}-{language} with score {j}")
                                self.to_excel(result, title, j, language)
                                reviews_count += len(result)
                                logger.info(f"Total {reviews_count} reviews for {title} collected")
                                break

                        except Exception as e:
                            logger.error(f"An error occurred while collecting reviews for {title}-{j} {country}-{language}. SLEEP 300", exc_info=e)
                            sleep(300)
                            break
                vpn.kill()
                self.translate(title, j)
                    

    def to_excel(self, result, title, score, language):
        try:
            logger.info(f"Updating {title}-{score}.xlsx")
            df_existing = pd.read_excel(f'{self.path}/{title}-{score}.xlsx')
            df_new = pd.DataFrame({"Author": [x["userName"] for x in result],
                                "Rating": [x["score"] for x in result],
                                "Date": [x["at"] for x in result], 
                                "Text": [x["content"] for x in result],
                                "Thumbup": [x["thumbsUpCount"] for x in result],
                                "Reply": [x["replyContent"] for x in result],
                                "ReplyDate": [x["repliedAt"] for x in result],
                                "Language": language
                                })
            df_new.drop_duplicates(inplace=True)
            df_existing.drop_duplicates(inplace=True)
            df_combined = pd.concat([df_existing, df_new], ignore_index=False)
            df_existing = None
            df_new = None
            df_combined.to_excel(f'{self.path}/{title}-{score}.xlsx', index=False, engine="xlsxwriter")
            df_combined = None
            logger.info(f"{title}-{score}.xlsx updated")
        except:
            try:
                logger.info(f"Creating excel file for {title}-{score}.xlsx")
                df = pd.DataFrame({"Author": [x["userName"] for x in result],
                                    "Rating": [x["score"] for x in result],
                                    "Date": [x["at"] for x in result], 
                                    "Text": [x["content"] for x in result],
                                    "Thumbup": [x["thumbsUpCount"] for x in result],
                                    "Reply": [x["replyContent"] for x in result],
                                    "ReplyDate": [x["repliedAt"] for x in result],
                                    "Language": language
                                    })
                df.drop_duplicates(inplace=True)
                df.to_excel(f'{self.path}/{title}-{score}.xlsx', index=False, engine="xlsxwriter")
                df = None
                logger.info(f"{title}-{score}.xlsx created")
            except Exception as e:
                logger.error("Error creating excel file", exc_info=e)

    def translate(self, title, score):
        try:
            j = 0
            logger.info(f"Translating {title}-{score}.xlsx")
            df = pd.read_excel(f'{self.path}/{title}-{score}.xlsx')
            text = df.Text.tolist()
            for i in range(len(text)):
                if i % 100000 == 0:
                    logger.info(f"Total translated {j}")
                try:
                    text[i] = ts.translate_text(text[i], translator = 'yandex', to_language="ru")
                except:
                    logger.warning(f"Cant translate line {i} in {title}-{score}, text --- {text[i]}")
                j += 1

            logger.info(f"{title}-{score}.xlsx translated, total - {j}")
            logger.info(f"Creating excel file - translated#{title}-{score}.xlsx")
            df["Text"] = text
            df.to_excel(f'{self.path}/translated#{title}-{score}.xlsx', index=False, engine="xlsxwriter")
            df = None
            logger.info(f"Excel file - translated#{title}-{score}.xlsx created")
            logger.log(60, f"{title} with score {score} complete!")
        except Exception as e:
            logger.error("Error translating excel file", exc_info=e)


def read_links():
    links: List[str] = []
    with open('links.txt', 'r') as f:
        for line in f.readlines():
            links.append(line.replace('\n', ''))
    return links

def main():
    path = './Output'
    if not os.path.exists(path):
        os.makedirs(path)

    links = read_links()
    try:
        ParseReviews(links, path).scrape()
    except Exception as e:
        logger.critical("A critical error occurred", exc_info=e)

    logger.log(70, "==========End==========")
    

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.critical("==========Keyboard interrupt==========")
        exit(0)