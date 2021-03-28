import json
from tweepy.streaming import StreamListener
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from textblob import TextBlob #predict the sentiment of Tweet, see 'https://textblob.readthedocs.io/en/dev/'
from elasticsearch import Elasticsearch,helpers 
import datetime
import calendar
import numpy as np
from json import loads, dumps
import csv
import geocoder
import yfinance as yf
from http.client import IncompleteRead
import tweepy as tw


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
            
            print(sentiment, tweet.sentiment.polarity, dict_data["text"])
            
    
            if len(dict_data["entities"]["hashtags"])>0:
                hashtags=dict_data["entities"]["hashtags"][0]["text"].title()
            else:
                hashtags="None"
                      
            es.index(index="logstash-a",
                     doc_type="test-type",
                     body={"author": dict_data["user"]["screen_name"],
                           "followers":dict_data["user"]["followers_count"],
                           #parse the milliscond since epoch to elasticsearch and reformat into datatime stamp in Kibana later
                           "date": datetime.strptime(dict_data["created_at"], '%a %b %d %H:%M:%S %z %Y'),
                           "message": dict_data["text"]  if "text" in dict_data.keys() else " ",
                           "hashtags":hashtags,
                           "polarity": tweet.sentiment.polarity,
                           "subjectivity": tweet.sentiment.subjectivity,
                           "sentiment": sentiment})
        return True
        
def on_error(self, status):
    print(status)


def get_stock():
    data = yf.download('JBLU','2019-01-01','2019-10-01')
    df = data[['Close']]
    df.reset_index(inplace=True,drop=False)

    df_iter = df.iterrows()


    with open('fuel.csv') as fh:
        rd = csv.DictReader(fh, delimiter=',')
        for row in rd:
            r = loads(dumps(row))
            es.index(index="fuel-prices",
                    doc_type="test-type1",
                    body={
                        "date": datetime.strptime(r["date"], '%b %Y'),
                        "price": float(r['price'])})


    for index, document in df_iter:
            es.index(index="stock-a",
                doc_type="test-type",
                body={
                "date": document["Date"],
                "close_price": float(document["Close"])})

def tripadvisor():
    rd = csv.DictReader(open('tripadv.csv'), delimiter='\t')
    data = [dict(d) for d in rd]
    try:
        for row in data:
            blob = TextBlob(row["text"])
            if blob:
                if blob.sentiment.polarity < 0:
                    sentiment = "negative"
                elif blob.sentiment.polarity == 0:
                    sentiment = "neutral"
                else:
                    sentiment = "positive"
            
                
            row.update({'source':row['location'].split(' - ')[0],
                        'dest':row['location'].split(' - ')[1],
                        "polarity": blob.sentiment.polarity,
                        "sentiment": sentiment}
                        )
            src = geocoder.osm(row['source']).latlng
            dest = geocoder.osm(row['dest']).latlng

            mapping = {
            "mappings": {
                    "properties": {
                        "rtext": {
                            "type": "keyword"
                        },
                        "j_type": {
                            "type": "keyword"
                        },
                        "class": {
                            "type": "keyword"
                        },
                        "source": {
                            "type": "keyword"
                        },
                        "dest": {
                            "type": "keyword"
                        },
                        "source_geo": {
                            "type": "geo_point"
                        },
                        "dest_geo": {
                            "type": "geo_point"
                        },
                
                }
            }
        }

            es.indices.create(index='tripadv', body=mapping, ignore=400)

            es.index(index="tripadv",
            body={
                'rtext':row['text'],
                'j_type':row['type'],
                'class':row['class'],
                'source':row['source'],
                'dest':row['dest'],
                'source_geo':{'lat':src[0],'lon':src[1]},
                'dest_geo':{'lat':dest[0],'lon':dest[1]},
                "polarity": blob.sentiment.polarity,
                "sentiment": sentiment
                }
                )

            print(row)
    except:
        pass

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
        


if __name__ == '__main__':
    listener = TweetStreamListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)    
    # tripadvisor()
    # tweets = tweepy.Cursor(api.search, q ="@JetBlue -brown -tantum").items(10000)
  
    # for tweet in tweets:
    #     print(tweet.user.location)
    #     singleAnalyzeTwitter(tweet._json)

    while True:
        try:
            stream = Stream(auth, listener)
            stream.filter(track=['jetblue'])
            tweets = tw.Cursor(api.search,
              q="jetblue",
              lang="en",
              since=datetime.datetime.today()).items(15)
        except IncompleteRead:
            continue
        except KeyboardInterrupt:
            stream.disconnect()
            break

