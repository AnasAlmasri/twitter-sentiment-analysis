
import tweepy
import sys
import simplejson
import pickle
import jsonpickle
import time
import os

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
fName = 'A:/tweets.json' # We'll store the tweets in a text file.

# If results from a specific ID onwards are reqd, set since_id to that ID.
# else default to no lower limit, go as far back as API allows
sinceId = None

# If results only below a specific ID are, set max_id to that ID.
# else default to no upper limit, start from the most recent tweet matching the search query.
max_id = -1
tweetCount = 0
print("Downloading max {0} tweets".format(maxTweets))
with open(fName, 'w') as f:
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
                    print(tweet)
                    f.write(jsonpickle.encode(tweet._json, unpicklable=False) + '\n')
                tweetCount += len(new_tweets)
                #print("Downloaded {0} tweets".format(tweetCount))
                max_id = new_tweets[-1].id
            except tweepy.TweepError as e:
                # Just exit if any error
                print("some error : " + str(e))
                break

    print("Downloaded {0} tweets, Saved to {1}".format(tweetCount, fName))



