from kafka import KafkaProducer
import kafka
import json
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

# TWITTER API CONFIGURATIONS
consumer_key = "iFzbYcHWr9k6af1B6DNLDLr7J"
consumer_secret = "ElvhzWzGWuBTprfTPPsOPdQHU7HwUFuoXriwHjLG8PEzjDKJux"
access_token = "1250631902605836290-GGN1jdCMQ7QXJvg0CQ5w4E8N9Uh1LF"
access_secret = "ihuvwjkzBEu4gIiS7mXsbXdI6uENgonjTUTOAEIa9g3PG"

# TWITTER API AUTH
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)


# Twitter Stream Listener
class KafkaPushListener(StreamListener):
    def __init__(self):
        # localhost:9092 = Default Zookeeper Producer Host and Port Adresses
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    # Get Producer that has topic name is Twitter
    # self.producer = self.client.topics[bytes("twitter")].get_producer()

    def on_data(self, data):
        # Producer produces data for consumer
        # Data comes from Twitter
        self.producer.send("twitter", data.encode('utf-8'))
        print(data)
        return True

    def on_error(self, status):
        print(status)
        return True


# Twitter Stream Config
twitter_stream = Stream(auth, KafkaPushListener())

# Produce Data that has trump hashtag (Tweets)
# To change hashtag between trump and coronavirus replace below hashtag with one which you need to visualize
twitter_stream.filter(track=['#coronavirus'])