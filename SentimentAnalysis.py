import json
import re
from string import punctuation

import bigquery
import tweepy
import nltk
from googleapiclient.discovery import build
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

# initialize Tweepy keys
from oauth2client.client import GoogleCredentials
from pandas_gbq import read_gbq

consumer_key = 'fOK9tgNpP2a0Mgm7lgNNhT1Tg'
consumer_secret = 'WlD7RfW5P09L9OPLvFFzVAchamwdZdUyCHKoNZkkcm7l9B5nyo'
access_token_key = '2224993461-DSs2wgPxtHh3xMhvxDtTdolgqVS4Ri93wAAMdt4'
access_token_secret = 'GsGNDt7NMIpojE8jcVTMdpBsesvdQogtMJ9Wxrh7a0nzs'

# authenticate Tweepy
authentication = tweepy.OAuthHandler(consumer_key, consumer_secret)
authentication.set_access_token(access_token_key, access_token_secret)
api = tweepy.API(authentication)

testData = []

from google.cloud import bigquery

#import pip
#pip.main(['install', 'pandas-gbq'])

def bigquerytestset():
    bigquery_client = bigquery.Client.from_service_account_json(
        'A:/Google Cloud Platform/My Project-9d7a4af36792.json')

    query = 'SELECT * FROM `SentimentAnalysis.testdataset`'
    query_job = bigquery_client.query(query)

    # Print the results.
    for row in query_job.result():  # Waits for job to complete.
        print(row["text"])

bigquerytestset()


def createTestData(in_string):  # function to build the test dataset
    # this function searches for a keyword in a set of tweets (local database)
    # it returns tweets that contain the search keyword (test dataset)

    file_name = "A:/tweets.json"  # json file from which tweets are retrieved
    temp_string = []  # used to hold the input string temporarily

    # make sure the input string comprises of characters
    for i in in_string:
        temp_string.append(i)
    search_term = ''.join(temp_string[1:len(temp_string)-1])

    count = 1  # fetched tweets counter
    with open(file_name, 'r') as f:
        print("Searching for tweets...")
        for tweet in f:
            t = json.loads(tweet)
            if search_term in t["text"]:
                testData.append({"text": t["text"], "label": "neutral"})
                count += 1
    print(str(count) + " Tweets found.\n")
#END OF FUNCTION


search_string = input("Enter search keyword: ")  # prompt user for search keyword
createTestData(search_string)  # build test dataset

count = 1
for i in testData:
    print(i)
    print(count)
    count += 1

def createTrainingCorpus(corpusFile):  # function to build training dataset
    # this function retrieves X trained tweets from the training dataset
    # parameter corpusFile is the json file containing all the trained tweets

    trainingData = []

    with open(corpusFile, 'r') as f:
        print("Fetching Training Data...")
        i = 0
        for tweet in f:
            if i == 2000:
                break
            t = json.loads(tweet)
            if t["label"] == "0":
                trainingData.append({"tweet_id": t["tweet_id"], "label": "negative", "text": t["text"]})
            else:
                trainingData.append({"tweet_id": t["tweet_id"], "label": "positive", "text": t["text"]})
            i += 1
    print(str(i) + " Tweets fetched.\n")
    return trainingData
# END OF FUNCTION


corpusFile = 'A:/SentimentAnalysisDataset.json'
trainingData = createTrainingCorpus(corpusFile)  # build training dataset


class PreProcessTweets:
    # this class preprocesses all the tweets, both test and training
    # it uses regular expressions and Natural Language Toolkit

    def __init__(self):
        self._stopwords = set(stopwords.words('english') + list(punctuation) + ['AT_USER', 'URL'])
    # END OF FUNCTION

    def processTweets(self, list_of_tweets):
        # the following is a list of dictionaries which have the keys "text" and "label"
        # this list is a list of tuples. Each tuple is a tweet (list of words) and its label
        processedTweets = []
        for tweet in list_of_tweets:
            processedTweets.append((self._processTweet(tweet["text"]), tweet["label"]))
        return processedTweets
    # END OF FUNCTION

    def _processTweet(self, tweet):
        # convert to lower case
        tweet = tweet.lower()
        # replace links with the word 'URL'
        tweet = re.sub('((www\.[^\s]+)|(https?://[^\s]+))', 'URL', tweet)
        # replace @username with 'AT_USER'
        tweet = re.sub('@[^\s]+', 'AT_USER', tweet)
        # replace #word with 'word'
        tweet = re.sub(r'#([^\s]+)', r'\1', tweet)
        # tokenize the tweet into a list of words
        tweet = word_tokenize(tweet)
        # return the tokenized tweet excluding all the stop words from it
        return [word for word in tweet if word not in self._stopwords]
    # END OF FUNCTION
# END OF CLASS


tweetProcessor = PreProcessTweets()
print("Preprocessing Training Data...")
ppTrainingData = tweetProcessor.processTweets(trainingData)
print("Preprocessing Test Data...")
ppTestData = tweetProcessor.processTweets(testData)


def buildVocabulary(ppTrainingData):
    # the following will give a list in which all the words in all the tweets are present
    # these have to be de-duped. Each word occurs in this list as many times as it appears in the corpus
    all_words = []
    for (words, sentiment) in ppTrainingData:
        all_words.extend(words)

    wordlist = nltk.FreqDist(all_words)  # create a dictionary with each word and its frequency
    word_features = wordlist.keys()

    # return the unique list of words in the corpus
    return word_features
# END OF FUNCTION

def extract_features(tweet):
    # function to take each tweet in the training data and represent it with
    # the presence or absence of a word in the vocabulary

    tweet_words = set(tweet)
    features = {}
    for word in word_features:
        features['contains(%s)' % word] = (word in tweet_words)
        # this gives a dictionary with keys like 'contains word' and values as True or False
    return features
# END OF FUNCTION


# extract the features and train the classifier
print("Building vocabulary for Training Data...")
word_features = buildVocabulary(ppTrainingData)
print("Classifying features...")
trainingFeatures = nltk.classify.apply_features(extract_features, ppTrainingData)

# train using Naive Bayes
print("Running Data on Naive Bayes Classifier...")
NBayesClassifier = nltk.NaiveBayesClassifier.train(trainingFeatures)

# run the classifier on the preprocessed test dataset
print("Acquiring Results...")
NBResultLabels = [NBayesClassifier.classify(extract_features(tweet[0])) for tweet in ppTestData]

print(NBResultLabels)
print("")
# get the majority vote and print the sentiment
if NBResultLabels.count('positive') > NBResultLabels.count('negative'):
    print("NB Result Positive Sentiment " + str(100 * NBResultLabels.count('positive') / len(NBResultLabels))+"%")
else:
    print("NB Result Negative Sentiment " + str(100 * NBResultLabels.count('negative') / len(NBResultLabels))+"%")




