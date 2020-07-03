import csv
import datetime
import json
import logging
import os

from google.cloud import storage
from twitter_scraper import get_tweets

log = logging.getLogger()

bucket = os.getenv("bucket")
data_path = os.getenv("data_path")
local_scraped_file = data_path + "/data.csv"
local_keywords_file = data_path + "/keywords.json"
remote_keywords_file = "keywords/keywords.json"
remote_scraped_data_path = "tweets/"

fields = [
    "tweetId",
    "word",
    "text",
    "isRetweet",
    "replies",
    "retweets",
    "likes",
    "time",
]

client = storage.Client()
bucket = client.bucket(bucket)
blob = bucket.get_blob(remote_keywords_file)
with open(local_keywords_file, "wb") as file_:
    blob.download_to_file(file_)

with open(local_keywords_file) as file_:
    keywords_dict = json.load(file_)
keywords = keywords_dict["keywords"]

for word in keywords:
    log.info("Starting scrape for %s" % word)
    tweets = get_tweets(word, pages=10)
    try:
        for tweet in tweets:
            row = {
                "tweetId": tweet["tweetId"],
                "word": word,
                "text": [tweet["text"]],
                "isRetweet": tweet["isRetweet"],
                "replies": tweet["replies"],
                "retweets": tweet["retweets"],
                "likes": tweet["likes"],
                "time": tweet["time"],
            }
            with open(local_scraped_file, "a", newline="") as csvfile:
                csvwriter = csv.DictWriter(csvfile, fieldnames=fields, delimiter=",")
                csvwriter.writerow(row)
            del row
    except Exception as e:
        log.error(e)
        continue

now = datetime.datetime.now().strftime("%m-%d-%Y-%H:%M:%S")
blob = bucket.blob(remote_scraped_data_path + now + ".csv")
with open(local_scraped_file, "rb") as file_:
    blob.upload_from_file(file_)
    os.remove(local_scraped_file)
    log.info("Scraped file uploaded to bucket")
