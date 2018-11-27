from kafka import KafkaProducer
import kafka
import json
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

# TWITTER API CONFIGURATIONS
consumer_key = "IM7VCBXFMKjcRKa1aTYDJ80iv"
consumer_secret = "Wczry0LcQb3jyQ242U1RG0ueSiDl703ERsgzwTifboPHn8nvDR"
access_token = "2552525262-UphOUS7Sun5B0ivHtZ8nZTiMZrzGqvK6exRlqQC"
access_secret = "4LSECwv5CtBx42VQwD8juIQyY2UBseYRQLgLCssLBV12E"

# TWITTER API AUTH
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)

hashtag="#trump"

# Twitter Stream Listener
class KafkaPushListener(StreamListener):
    def __init__(self):
        # localhost:9092 = Default Zookeeper Producer Host and Port Adresses
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])



    def on_data(self, data):
        # Producer produces data for consumer
        self.producer.send("twitter", data.encode('utf-8'))
        print(data)
        return True

    def on_error(self, status):
        print(status)
        return True

twitter_stream = Stream(auth, KafkaPushListener())
twitter_stream.filter(track=[hashtag])
