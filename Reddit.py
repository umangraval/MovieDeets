import os
import praw
from praw.models import MoreComments
import pandas as pd
import datetime as dt
from textblob import TextBlob
from elasticsearch import Elasticsearch, helpers
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()


es = Elasticsearch()

reddit = praw.Reddit(client_id=os.environ.get("client_id"),
                     client_secret=os.environ.get("client_secret"),
                     user_agent="SIN-PROJECT",
                     username=os.environ.get("username"),
                     password=os.environ.get("password"))

movies_sub = reddit.subreddit('movies')
new = movies_sub.new(limit=100)


# for comments in movies_sub.stream.submissions():
#     print(comments)
#     # comment = TextBlob(comments.body)
#     # print(comment.sentiment.polarity)


# get static review for a particular movie from all time
# for submission in movies_sub.search("Godzilla", limit=5):
#     print(submission.title)
#     submission_polarity = 0
#     comments = submission.comments.list()
#     for c in comments:
#         if isinstance(c, MoreComments):
#             continue
#         print(c.body)
#         comment = TextBlob(c.body)
#         if comment:
#             print(comment.sentiment.polarity)
#             submission_polarity += comment.sentiment.polarity

#     mapping = {
#         "mappings": {
#             "properties": {
#                 "author": {
#                     "type": "keyword"
#                 },
#                 "date": {
#                     "type": "date"
#                 },
#                 "message": {
#                     "type": "keyword"
#                 },
#                 "polarity": {
#                     "type": "keyword"
#                 },
#                 "sentiment": {
#                     "type": "keyword"
#                 },
#             }
#         }
#     }
#     if submission_polarity < 0:
#         sentiment = "negative"
#     elif submission_polarity == 0:
#         sentiment = "neutral"
#     else:
#         sentiment = "positive"
#     es.indices.create(index='logstash-reddit', body=mapping, ignore=400)
#     print(submission_polarity)

#     es.index(index="logstash-reddit",
#              #  doc_type="test-type",
#              body={"author": submission.author.name,
#                    # parse the milliscond since epoch to elasticsearch and reformat into datatime stamp in Kibana later
#                    "date": submission.created_utc,
#                    "message": submission.title,
#                    "polarity": submission_polarity,
#                    "sentiment": sentiment,
#                    })

# get live updates on a movie review
for submission in reddit.subreddit("askreddit").stream.submissions():
    print(submission.title)
    s = TextBlob(submission.title)
    submission_polarity = s.sentiment.polarity
    mapping = {
        "mappings": {
            "properties": {
                "author": {
                    "type": "keyword"
                },
                "date": {
                    "type": "date"
                },
                "message": {
                    "type": "keyword"
                },
                "polarity": {
                    "type": "keyword"
                },
                "sentiment": {
                    "type": "keyword"
                },
            }
        }
    }
    if submission_polarity < 0:
        sentiment = "negative"
    elif submission_polarity == 0:
        sentiment = "neutral"
    else:
        sentiment = "positive"
    # es.indices.create(index='logstash-live-reddit', body=mapping, ignore=400)
    print(submission_polarity)

    es.index(index="logstash-live-reddit",
             doc_type="test-type",
             body={"author": submission.author.name,
                   # parse the milliscond since epoch to elasticsearch and reformat into datatime stamp in Kibana later
                   "date": submission.created_utc,
                   "message": submission.title,
                   "polarity": submission_polarity,
                   "sentiment": sentiment,
                   })
