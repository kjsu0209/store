import io
import oauth2
import json
import datetime
from pymongo import MongoClient

import GetOldTweets3 as got
import time
#from config import *

#[CODE 1]
def oauth2_request(consumer_key, consumer_secret, access_token, access_secret):
    try:
        consumer = oauth2.Consumer(key=consumer_key, secret=consumer_secret)
        token = oauth2.Token(key=access_token, secret=access_secret)
        client = oauth2.Client(consumer, token)
        return client
    except Exception as e:
        print(e)
        return None

#[CODE 2]
def get_user_timeline(client, screen_name, count=50, since_id=-1, include_rts='False'):
    base = "https://api.twitter.com/1.1"
    node = "/statuses/user_timeline.json"
    #fields = "?screen_name=%s" % (screen_name)
    if(since_id == -1):
        fields = "?screen_name=%s&count=%s&include_rts=%s" % (screen_name, count, include_rts)
    else:
        fields = "?screen_name=%s&count=%s&max_id=%s&include_rts=%s" % (screen_name, count, since_id, include_rts)
    url = base + node + fields
    response, data = client.request(url)
    try:
        if response['status'] == '200':
            return json.loads(data.decode('utf-8'))
    except Exception as e:
        print(e)
        return None

#[CODE 3]
def getTwitterTwit(tweet, jsonResult, db):

    tweet_id = str(tweet.id)
    tweet_message = tweet.text

    screen_name = tweet.username
    tweet_link = tweet.permalink
    hashtags = tweet.hashtags
    tweet_published = tweet.date

    num_favorite_count = tweet.favorites
    num_comments = tweet.mentions
    num_shares = tweet.retweets
    num_likes = num_favorite_count
    num_loves = num_wows = num_hahas = num_sads = num_angrys = 0

    jsonToAppend = {'post_id':tweet_id, 'message':tweet_message,
                    'name':screen_name, 'link':tweet_link,
                    'created_time':tweet_published, 'num_reactions':num_favorite_count,
                    'num_comments':num_comments, 'num_shares':num_shares,
                    'num_likes':num_likes, 'num_loves':num_loves,
                    'num_wows':num_wows, 'num_hahas':num_hahas,
                    'num_sads':num_sads, 'num_angrys':num_angrys, 'hashtags': hashtags}

    #update db
    db[screen_name].replace_one({'post_id':str(tweet.id)}, jsonToAppend, upsert=True)

    jsonResult.append(jsonToAppend)

##initialize db
def init_db():
    client = MongoClient('localhost', 27017)
    # localhost: ip주소
    # 27017: port 번호
    db = client["DB_tweet"]
    return db

##get_tweets.py
def main():
    screen_name = ["joongangilbo"]
   # screen_name = ["kyunghyang", "Chosun", "joongangilbo"]
    CONSUMER_KEY = ""
    CONSUMER_SECRET = ""
    ACCESS_TOKEN = ""
    ACCESS_SECRET = ""
    num_posts = 100000 #max tweets per request
    start_date = "2017-01-01"
    end_date = "2019-06-30"
    
    client = oauth2_request(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)

    #initialize db
    db = init_db() 

    for name in screen_name:
        yeargap = int(end_date[0:4]) - int(start_date[0:4])
        for i in range(yeargap+1):
            year = int(start_date[0:4]) + i
            if(i == 0 and yeargap == 0):
               read_tweets(name, start_date, end_date, num_posts, db)
            elif(i == 0 and yeargap != 0):
               read_tweets(name, start_date,str(year) + '-12-31', num_posts, db)
            elif(i == yeargap):
               read_tweets(name,str(year) + '-01-01', end_date, num_posts, db)
            else:
               read_tweets(name,str(year) + '-01-01', str(year) + '-12-31', num_posts, db)


def read_tweets(username, start_date, end_date, num_posts, db):
    jsonResult = []
    tweetCriteria = got.manager.TweetCriteria().setUsername(username)\
        .setSince(start_date)\
        .setUntil(end_date)\
        .setMaxTweets(num_posts)
    tweets = got.manager.TweetManager.getTweets(tweetCriteria)
    for tweet in tweets:
        getTwitterTwit(tweet, jsonResult, db)
    print(username, '(', start_date, '~', end_date, ')\n','loaded data: ', len(tweets))

if __name__ == '__main__':
    main()
