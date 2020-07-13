import boto3
import json
from datetime import datetime
import calendar
import random
import time
import credentials
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream


class TweetStreamListener(StreamListener):
    # on success
    def on_data(self, data):

        # decode json
        tweet = json.loads(data)
        # print(tweet)
        if "text" in tweet.keys():
            payload = {'id': str(tweet['id']),
                        'tweet': str(tweet['text'].encode('utf8', 'replace')),
                        'ts': str(tweet['created_at']),
                        'place': str(tweet['place']['country']),
                        },
            print(payload)
            try:
                put_response = kinesis_client.put_record(
                                StreamName=stream_name,
                                Data=json.dumps(payload),
                                PartitionKey=str(tweet['user']['screen_name']))
            except (AttributeError, Exception) as e:
                print (e)
                pass
        return True

    # on failure
    def on_error(self, status):
        print(status)


stream_name = 'tweets'  # fill the name of Kinesis data stream you created

if __name__ == '__main__':
    # create kinesis client connection
    kinesis_client = boto3.client('kinesis',
                                  region_name='us-west-2',  # enter the region
                                  aws_access_key_id=credentials.AWS_ACCESSS_KEY,
                                  aws_secret_access_key=credentials.AWS_SECRET_KEY)
    # create instance of the tweepy tweet stream listener
    listener = TweetStreamListener()
    # set twitter keys/tokens
    auth = OAuthHandler(credentials.API_KEY, credentials.API_SECRET_KEY)
    auth.set_access_token(credentials.ACCESS_TOKEN, credentials.ACCESS_TOKEN_SECRET)
    # create instance of the tweepy stream
    stream = Stream(auth, listener)
    # search twitter for tags or keywords from cli parameters
    track = ['#DonaldTrump', 'Trump']
    stream.filter(track=track)
