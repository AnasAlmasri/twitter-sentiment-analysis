
import tweepy
import sys
import pickle
import jsonpickle
import time
import os
import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from string import punctuation

API_KEY = "fOK9tgNpP2a0Mgm7lgNNhT1Tg"
API_SECRET = "WlD7RfW5P09L9OPLvFFzVAchamwdZdUyCHKoNZkkcm7l9B5nyo"

auth = tweepy.AppAuthHandler(API_KEY, API_SECRET)

api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

if (not api):
    print ("Can't Authenticate")
    sys.exit(-1)

searchList = ['wheat', 'rice', 'corn', 'soybean', 'tomato',
             'sugarcane', 'grape', 'cotton', 'apple', 'banana']
searchIndex = -1
maxTweets = 10000000 # Some arbitrary large number
tweetsPerQry = 180  # this is the max the API permits
# If results from a specific ID onwards are reqd, set since_id to that ID.
# else default to no lower limit, go as far back as API allows
sinceId = None

def processTweet(tweet):
    tweet = tweet.lower()  # convert to lower case
    tweet = re.sub('((www\.[^\s]+)|(https://[^\s]+))', 'URL', tweet)  # replace links with the word 'URL'
    tweet = re.sub('@[^\s]+', 'AT_USER', tweet)  # replace @username with 'AT_USER'
    tweet = re.sub(r'#([^\s]+)', r'\1', tweet)  # replace #word with 'word'
    tweet = word_tokenize(tweet)  # tokenize the tweet into a list of words
    sw = set(stopwords.words('english') + list(punctuation) + ['AT_USER', 'URL'])
    # return the tokenized tweet excluding all the stop words from it
    return [word for word in tweet if word not in sw]

from google.cloud import bigquery
bigquery_client = bigquery.Client.from_service_account_json(
        'A:/Google Cloud Platform/My Project-9d7a4af36792.json')

# If results only below a specific ID are, set max_id to that ID.
# else default to no upper limit, start from the most recent tweet matching the search query.
max_id = -1
tweetCount = 0

while True:
    while tweetCount < maxTweets:
        searchIndex = -1
        while searchIndex < 9:
            searchIndex += 1
            searchQuery = searchList[searchIndex]
            try:
                if max_id <= 0:
                    if not sinceId:
                        new_tweets = api.search(q=searchQuery, count=tweetsPerQry)
                    else:
                        new_tweets = api.search(q=searchQuery, count=tweetsPerQry,
                                                since_id=sinceId)
                else:
                    if not sinceId:
                        new_tweets = api.search(q=searchQuery, count=tweetsPerQry,
                                                max_id=str(max_id - 1))
                    else:
                        new_tweets = api.search(q=searchQuery, count=tweetsPerQry,
                                                max_id=str(max_id - 1),
                                                since_id=sinceId)
                if not new_tweets:
                    print("No more tweets found")
                    break
                for tweet in new_tweets:
                    # prepare text and insert it into BigQuery dataset
                    t = (tweet._json)["text"]
                    t = processTweet(t)
                    t = ' '.join(t)
                    query = 'INSERT SentimentAnalysis.testdataset (text) VALUES({})'
                    query = query.format("'"+t+"'")
                    query_job = bigquery_client.query(query)
                max_id = new_tweets[-1].id
            except tweepy.TweepError as e:
                # Just exit if any error
                print("some error : " + str(e))
                break
