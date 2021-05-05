import json
from tweepy.streaming import StreamListener
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from textblob import TextBlob #predict the sentiment of Tweet, see 'https://textblob.readthedocs.io/en/dev/'
from elasticsearch import Elasticsearch,helpers 
import datetime
from datetime import datetime
import calendar
import numpy as np
from json import loads, dumps
import csv
import geocoder
import yfinance as yf
from http.client import IncompleteRead
import tweepy as tw
import tkinter as tk



# consumer_key = '<Twitter_Consumer_Key>'
# consumer_secret = '<Twitter_Consumer_Secret>'
# access_token = '<Twitter_Access_Token>'
# access_token_secret = '<Twitter_Access_Token_Secret>'

consumer_key = "2bqLbdibv3Gs8xIYYpwluzkSG"
consumer_secret = "cfQAOv6RohJUYP3UIz3iETfRIYo3Ua3NvKM8NriHMjRSATFkP9"
access_token = "1374563238772273152-mqYqtsLQG9u1GIPnWeSQkNAsJDlYzT"
access_token_secret = "vUESdHBtnzhpwlWgeCguhVwqaoALEgUipVTO3HYoAFrLA"


# create instance of elasticsearch
es = Elasticsearch()

auth = tweepy.OAuthHandler(consumer_key,consumer_secret)
auth.set_access_token(access_token,access_token_secret)
api = tweepy.API(auth)

score = {
    "negative": 1,
    "neutral": 6,
    "positive": 9,
}
# twitter responses
class TweetStreamListener(StreamListener):
      def on_data(self, data):
        dict_data = json.loads(data)
        tweet = TextBlob(dict_data["text"]) if "text" in dict_data.keys() else None
        if tweet:
            if tweet.sentiment.polarity < 0:
                sentiment = "negative"
            elif tweet.sentiment.polarity == 0:
                sentiment = "neutral"
            else:
                sentiment = "positive"
            
            src = geocoder.osm(dict_data["user"]["location"]).latlng
            if src == None:
                src = [0,0]
                        
            if len(dict_data["entities"]["hashtags"])>0:
                hashtags=dict_data["entities"]["hashtags"][0]["text"].title()
            else:
                hashtags="None"

            mapping = {
            "mappings": {
                    "properties": {
                        "author": {
                            "type": "keyword"
                        },
                        "followers": {
                            "type": "keyword"
                        },
                        "date": {
                            "type": "date"
                        },
                        "message": {
                            "type": "keyword"
                        },
                        "hashtags": {
                            "type": "keyword"
                        },
                        "polarity": {
                            "type": "keyword"
                        },
                        "subjectivity": {
                            "type": "keyword"
                        },
                        "sentiment": {
                            "type": "keyword"
                        },
                        "place": {
                            "type": "keyword"
                        },
                        "location": {
                            "type": "geo_point"
                        },
                        "rating": {
                            "type": "number"
                        },
                }
            }
        }

            es.indices.create(index='logstash-movie', body=mapping, ignore=400)
            print(sentiment, dict_data["text"], dict_data["user"]["location"])
            # prev = es.search(index='logstash-movie', size=1, sort='date:desc')
            # prev_rating = prev["hits"]["hits"][0]["_source"]["rating"]
            # update_rating = (prev_rating + score[sentiment])/2
            # print(prev_rating, score[sentiment], update_rating)
            es.index(index="logstash-movie",
                    #  doc_type="test-type",
                     body={"author": dict_data["user"]["screen_name"],
                           "followers":dict_data["user"]["followers_count"],
                           #parse the milliscond since epoch to elasticsearch and reformat into datatime stamp in Kibana later
                           "date": datetime.strptime(dict_data["created_at"], '%a %b %d %H:%M:%S %z %Y'),
                           "message": dict_data["text"]  if "text" in dict_data.keys() else " ",
                           "hashtags":hashtags,
                           "polarity": tweet.sentiment.polarity,
                           "subjectivity": tweet.sentiment.subjectivity,
                           "sentiment": sentiment,
                           "place": dict_data["user"]["location"],
                           "location": {'lat':src[0],'lon':src[1]},
                           "rating": score[sentiment]})
        
        
        return True
        
def on_error(self, status):
    print(status)


# def webscrape():
#     rd = csv.DictReader(open('tripadv.csv'), delimiter='\t')
#     data = [dict(d) for d in rd]
#     try:
#         for row in data:
#             blob = TextBlob(row["text"])
#             if blob:
#                 if blob.sentiment.polarity < 0:
#                     sentiment = "negative"
#                 elif blob.sentiment.polarity == 0:
#                     sentiment = "neutral"
#                 else:
#                     sentiment = "positive"
            
                
#             row.update({'source':row['location'].split(' - ')[0],
#                         'dest':row['location'].split(' - ')[1],
#                         "polarity": blob.sentiment.polarity,
#                         "sentiment": sentiment}
#                         )
#             src = geocoder.osm(row['source']).latlng
#             dest = geocoder.osm(row['dest']).latlng

#             mapping = {
#             "mappings": {
#                     "properties": {
#                         "rtext": {
#                             "type": "keyword"
#                         },
#                         "j_type": {
#                             "type": "keyword"
#                         },
#                         "class": {
#                             "type": "keyword"
#                         },
#                         "source": {
#                             "type": "keyword"
#                         },
#                         "dest": {
#                             "type": "keyword"
#                         },
#                         "source_geo": {
#                             "type": "geo_point"
#                         },
#                         "dest_geo": {
#                             "type": "geo_point"
#                         },
                
#                 }
#             }
#         }

#             es.indices.create(index='tripadv', body=mapping, ignore=400)

#             es.index(index="tripadv",
#             body={
#                 'rtext':row['text'],
#                 'j_type':row['type'],
#                 'class':row['class'],
#                 'source':row['source'],
#                 'dest':row['dest'],
#                 'source_geo':{'lat':src[0],'lon':src[1]},
#                 'dest_geo':{'lat':dest[0],'lon':dest[1]},
#                 "polarity": blob.sentiment.polarity,
#                 "sentiment": sentiment
#                 }
#                 )

#             print(row)
#     except:
#         pass

def singleAnalyzeTwitter(data):
    dict_data = data
    print(dict_data)
    
    tweet = TextBlob(dict_data["text"]) if "text" in dict_data.keys() else None
    if tweet:
        if tweet.sentiment.polarity < 0:
            sentiment = "negative"
        elif tweet.sentiment.polarity == 0:
            sentiment = "neutral"
        else:
            sentiment = "positive"
        
        print(sentiment, tweet.sentiment.polarity, dict_data["text"])
        

        if len(dict_data["entities"]["hashtags"])>0:
            hashtags=dict_data["entities"]["hashtags"][0]["text"].title()
        else:
            hashtags="None"
                    
        es.index(index="logstash-a",
                    doc_type="test-type",
                    body={"author": dict_data["user"]["screen_name"],
                        "followers":dict_data["user"]["followers_count"],
                        "date": datetime.strptime(dict_data["created_at"], '%a %b %d %H:%M:%S %z %Y'),
                        "message": dict_data["text"]  if "text" in dict_data.keys() else " ",
                        "hashtags":hashtags,
                        "polarity": tweet.sentiment.polarity,
                        "subjectivity": tweet.sentiment.subjectivity,
                        "sentiment": sentiment})
        
def getMovieName ():
    movie = entry1.get();
    label1 = tk.Label(root, text="Check Kibana For Visualization")
    canvas1.create_window(200, 230, window=label1)
    root.destroy()
    while True:
        try:
            stream = Stream(auth, listener)
            stream.filter(track=[movie])
            # tweets = tw.Cursor(api.search,
            #   q="mortal kombat",
            #   lang="en",
            #   since=datetime.datetime.today()).items(15)
        except IncompleteRead:
            continue
        except KeyboardInterrupt:
            stream.disconnect()
            break


if __name__ == '__main__':
    listener = TweetStreamListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)    
    # tweets = tweepy.Cursor(api.search, q ="@Mortal -brown -tantum").items(10000)
  
    # for tweet in tweets:
    #     print(tweet.user.location)
    #     singleAnalyzeTwitter(tweet._json)
    root= tk.Tk()
    canvas1 = tk.Canvas(root, width = 400, height = 300)
    canvas1.pack()

    label1 = tk.Label(root, text='Search data for the movie')
    label1.config(font=('helvetica', 14))
    canvas1.create_window(200, 25, window=label1)

    label2 = tk.Label(root, text='Enter movie name:')
    label2.config(font=('helvetica', 10))
    canvas1.create_window(200, 100, window=label2)

    entry1 = tk.Entry (root) 
    canvas1.create_window(200, 140, window=entry1)

    button1 = tk.Button(text='Add', command=getMovieName, bg='brown', fg='white', font=('helvetica', 9, 'bold'))
    canvas1.create_window(200, 180, window=button1)
    root.mainloop()

    

