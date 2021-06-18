import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json

#access keys for twitter
consumer_key    = "SlOw2tMaPsmYVDUMs1B7Sc7hD"
consumer_secret = "I3Cg7UOYXxmPGfI3jS0wLmvdLH1bUB3Eg8GhGuGFI32gZvHnLm"
access_token    = "1274643297651298304-b81dbU1UN7D07KQRx1yX6dWamWniYw"
access_secret   = "Xen3HRMfcZ6XSMtae15e4ONZZ60OsTinO3ZSsJkUUekoR"


class TweetsListener(StreamListener):
 
    def __init__(self, csocket):
        self.client_socket = csocket
 
    def on_data(self, data):
        try:
            message = json.loads(data)
            #saving the tweets to a csv file as am unstrcutured data format
            file = open("unstructured_DB.csv", "a")
            file.write(message['text'])
            file.write('\n')
            file.close()
            #this send the tweets through the tcp socket
            print(message['text'].encode('utf-8'))
            self.client_socket.send(message['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True
 
    def on_error(self, status):
        print(status)
        return True
 
def send_tweets(c_socket):
	#connecting to twitter using the access keys
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
 
    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=["#coronavirus", "#covid-19"], languages=["en"]) #only collecting tweets that have these hashtags in them
 
if __name__ == "__main__":
    scket = socket.socket()     
    host = "127.0.0.1"      
    port = 5555             
    scket.bind((host, port))    
 
    print("Listening on port: %s" % str(port))
 
    scket.listen(5)                 
    c, addr = scket.accept()        
 
    print("Received request from: " + str( addr ))
 
    send_tweets(c)
