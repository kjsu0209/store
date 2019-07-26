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
    screen_name = ["kyunghyang", "Chosun", "joongangilbo"]
    CONSUMER_KEY = "qZHgrr3O0knOt075c49LwYLcq"
    CONSUMER_SECRET = "LhB7M1cIqckNJw6EDTYCxOkeOQ1Gq8fkPIbcd5ril7qa1sEntF"
    ACCESS_TOKEN = "1147813462250123266-GSdmBBPVDWbiD6cqXcSluBawr0t7JB"
    ACCESS_SECRET = "kV4L39AIQn7aTsj6ldu8cfYc4H6JwQLVI9VEqvcvtDvZg"
    num_posts = 200 #max tweets per request : 200

    client = oauth2_request(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)

    #initialize db
    db = init_db()

    #kyunghyang
    jsonResult = []
    tweetCriteria = got.manager.TweetCriteria().setUsername("kyunghyang") \
        .setSince("2018-01-01") \
        .setUntil("2018-12-31") \
        .setMaxTweets(20000)
    tweets = got.manager.TweetManager.getTweets(tweetCriteria)
    for tweet in tweets:
        getTwitterTwit(tweet, jsonResult, db)
    print('loaded data: ', len(tweets))
    #Chosun
    jsonResult = []
    tweetCriteria = got.manager.TweetCriteria().setUsername("Chosun") \
        .setSince("2018-01-01") \
        .setUntil("2018-12-31") \
        .setMaxTweets(20000)
    tweets = got.manager.TweetManager.getTweets(tweetCriteria)
    for tweet in tweets:
        getTwitterTwit(tweet, jsonResult, db)
    print('loaded data: ', len(tweets))

    #joongangilbo
    jsonResult = []
    tweetCriteria = got.manager.TweetCriteria().setUsername("joongangilbo") \
        .setSince("2018-01-01") \
        .setUntil("2018-12-31") \
        .setMaxTweets(20000)
    tweets = got.manager.TweetManager.getTweets(tweetCriteria)
    for tweet in tweets:
        getTwitterTwit(tweet, jsonResult, db)
    print('loaded data: ', len(tweets))


if __name__ == '__main__':
    main()
