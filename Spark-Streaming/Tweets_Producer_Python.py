import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
import json

access_token = ''
access_token_secret = ''
consumer_key = ''
consumer_secret = ''

class StdOutListener(tweepy.Stream):
    def on_data(self, data):
        json_ = json.loads(data) 
        producer.send("streamingdata", json_["text"].encode('utf-8'))
        return True
    def on_error(self, status):
        print (status)

producer = KafkaProducer(bootstrap_servers='localhost:9092')
stream = StdOutListener(consumer_key,consumer_secret,access_token,access_token_secret)
stream.filter(track=["Hawaii"])
