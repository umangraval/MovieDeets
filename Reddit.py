import os
import praw
import pandas as pd
import datetime as dt

from dotenv import load_dotenv
load_dotenv()


reddit = praw.Reddit(client_id=os.environ.get("client_id"),
                     client_secret=os.environ.get("client_secret"),
                     user_agent="SIN-PROJECT",
                     username=os.environ.get("username"),
                     password=os.environ.get("password"))

movies_sub = reddit.subreddit('movies')
new = movies_sub.new(limit=100)

# for submission in movies_sub.search("Godzilla", limit=5):
#     for reply in submission.replies:
#         if reply.body.find(findme) != -1:
#             print(reply)

for comments in movies_sub.stream.comments(skip_existing=True):
    print(comments.body)
