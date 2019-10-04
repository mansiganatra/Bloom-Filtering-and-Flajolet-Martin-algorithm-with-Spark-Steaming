import sys
import json
from time import time
import math
import random
import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener




class MansiGanatraTweetsAnalyzer(StreamListener):
    def on_status(self, status):
        global n_tweets
        global all_tweets

        tags_frequency_dict = {}

        current_hashtags = status.entities.get("hashtags")
        if len(current_hashtags) > 0:
            n_tweets += 1
            if n_tweets < 100:
                all_tweets.append(status)
            else:
                random_tweet_index = random.randint(0, n_tweets)

                # keep each nth value with probability of 100/n if nth is kept,
                # discard one other random tweet
                if random_tweet_index < 100:
                    all_tweets[random_tweet_index] = status

                for tweet in all_tweets:
                    current_hashtags = tweet.entities.get("hashtags")

                    for hashtag in current_hashtags:
                        value = hashtag["text"]
                        if value not in tags_frequency_dict.keys():
                            tags_frequency_dict[value] = 1
                        else:
                            tags_frequency_dict[value] = tags_frequency_dict[value] + 1

                tweets_in_order = sorted(tags_frequency_dict.items(), key=lambda entry: (-entry[1], entry[0]))
                top3_tweets = tweets_in_order[0:3]

                print("\nThe number of tweets with tags from the begining: ", str(n_tweets))
                print("\n")
                for tweet in top3_tweets:
                    print(tweet[0]+": "+str(tweet[1]))

    def on_error(self, status_code):
        print("Error occurred while processing: ", str(status_code))


# Credentials created for my userid @swiftwave2

consumer_key = 'm4GFp6zBzkGxRH2uHBzQ4YHQh'
consumer_secret_key = 'itxcaAjpomM1oscHhmwWEO6LIjaRyBYxuvId9X902AEfg7oQjC'
access_token = '129242538-ck8erdoE8AdYReJ9REe8BGAk0oVY7ZbyA3eYrLuh'
access_secret_key = 'uVsXCR2ZEgZHNCXe5AsXfHSvttGgle5vxUAyaUVQcdg3X'

auth = tweepy.OAuthHandler(consumer_key=consumer_key, consumer_secret=consumer_secret_key)
auth.set_access_token(key=access_token, secret=access_secret_key)

api = tweepy.API(auth)
n_tweets = 0
all_tweets = []

# tweet_analyzer = MansiGanatraTweetsAnalyzer()
tweet_stream = Stream(auth=auth, listener=MansiGanatraTweetsAnalyzer())
# tweet_stream.filter(track=["Ryan", "Reynolds", "Deadpool", "Deadpool2", "Detective","Pikachu"])
tweet_stream.filter(track=["Trump", "Bush", "Obama", "AI"])