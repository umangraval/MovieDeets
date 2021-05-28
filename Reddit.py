import os
import praw
from praw.models import MoreComments
import pandas as pd
import datetime as dt
from textblob import TextBlob
from elasticsearch import Elasticsearch, helpers
from datetime import datetime
import tkinter as tk
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
def staticReview(movie_name):
    for submission in movies_sub.search(movie_name, limit=10):
        print(submission.title)
        submission_polarity = 0
        comment_count = 0
        comment_count_n = 0
        comment_count_p = 0

        comments = submission.comments.list()
        for c in comments:
            if isinstance(c, MoreComments):
                continue
            print(c.body)
            comment = TextBlob(c.body)
            if comment:
                comment_count += 1
                c_polarity = comment.sentiment.polarity
                if c_polarity < 0:
                    comment_count_n += 1
                elif c_polarity > 0:
                    comment_count_p += 1
                print(comment.sentiment.polarity)
                submission_polarity += comment.sentiment.polarity

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
                    "post_url": {
                        "type": "keyword"
                    },
                    "flairs": {
                        "type": "keyword"
                    },
                    "positive_comments": {
                        "type": "number"
                    },
                    "negative_comments": {
                        "type": "number"
                    }
                }
            }
        }
        if submission_polarity < 0:
            sentiment = "negative"
        elif submission_polarity == 0:
            sentiment = "neutral"
        else:
            sentiment = "positive"
        es.indices.create(index='logstash-reddit', body=mapping, ignore=400)
        print(submission_polarity/comment_count)

        es.index(index="logstash-reddit",
                #  doc_type="test-type",
                body={"author": submission.author.name,
                    # parse the milliscond since epoch to elasticsearch and reformat into datatime stamp in Kibana later
                    "date": submission.created_utc,
                    "message": submission.title,
                    "polarity": submission_polarity/comment_count,
                    "sentiment": sentiment,
                    "post_url": submission.url,
                    "flairs": submission.link_flair_text,
                    "positive_comments": comment_count_p,
                    "negative_comments": comment_count_n
                    })

# get live updates on a movie review

def streamComments(movie_name):
    for submission in reddit.subreddit("movies").stream.submissions():
        # remove to get all movies 
        # if movie_name not in submission.title:
        #     continue
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
        es.indices.create(index='logstash-live-reddit', body=mapping, ignore=400)
        print(submission_polarity)

        es.index(index="logstash-live-reddit",
                #  doc_type="test-type",
                body={"author": submission.author.name,
                    # parse the milliscond since epoch to elasticsearch and reformat into datatime stamp in Kibana later
                    "date": submission.created_utc,
                    "message": submission.title,
                    "polarity": submission_polarity,
                    "sentiment": sentiment,
                    })

def getMovieName():
    movie = entry1.get();
    label1 = tk.Label(root, text="Check Kibana For Visualization")
    canvas1.create_window(200, 230, window=label1)
    root.destroy()
    
    while True:
        try:
            staticReview(movie)
            # streamComments(movie)
        except KeyboardInterrupt:
            break
        except:
            continue


if __name__ == '__main__':
    root = tk.Tk()
    canvas1 = tk.Canvas(root, width=400, height=300)
    canvas1.pack()

    label1 = tk.Label(root, text='Search data for the movie')
    label1.config(font=('helvetica', 14))
    canvas1.create_window(200, 25, window=label1)

    label2 = tk.Label(root, text='Enter movie name:')
    label2.config(font=('helvetica', 10))
    canvas1.create_window(200, 100, window=label2)

    entry1 = tk.Entry(root)
    canvas1.create_window(200, 140, window=entry1)

    button1 = tk.Button(text='Add', command=getMovieName,
                        bg='brown', fg='white', font=('helvetica', 9, 'bold'))
    canvas1.create_window(200, 180, window=button1)
    root.mainloop()
