'''
Created on Nov 29, 2012

@author: raber
'''

from twitter import TwitterStream, TwitterHTTPError, OAuth
import threading
import logging
import time
from urllib2 import HTTPError
from Queue import Queue, Empty

class QueueHandler(threading.Thread):
    def __init__(self, msgQueue, dataStore):
        super(QueueHandler, self).__init__(name="QH")
        self.queue = msgQueue
        self.data = dataStore
        self.daemon = True
        self.do_run = True
    def run(self):
        logging.info("Starting")
        while(self.do_run):
            try:
                (hashtag, user_id, user_name, user_profile_img_url) = self.queue.get(block=True, timeout=10)
                logging.debug("Hashtag: %s Id: %d Name: %s Url: %s", hashtag, user_id, user_name, user_profile_img_url)
                self.data.insert(hashtag, user_id, user_name, user_profile_img_url)
                self.queue.task_done()
            except Empty:
                continue
        logging.info("Finishing")
                

class HashTagThread(threading.Thread):
    
    def __init__(self, hashtag, msgQueue, auth_config):
        super(HashTagThread, self).__init__(name="HTT<%s>"%hashtag)
        self.hashtag = hashtag
        self.queue = msgQueue
        self.daemon = True
        self.do_run = True 
        self.auth_config = auth_config
    
    def run(self):
        logging.info("Starting, hashtag: %s", self.hashtag)
        while(self.do_run):
            try:
                logging.debug("Connecting & authenticating")
                twitter_stream = TwitterStream(auth=OAuth(
                    token=self.auth_config['token'],
                    token_secret=self.auth_config['token_secret'],
                    consumer_key=self.auth_config['consumer_key'],
                    consumer_secret=self.auth_config['consumer_secret']))
                
                logging.debug("Connecting filtered stream")
                iterator = twitter_stream.statuses.filter(track=self.hashtag)
            
                for tweet in iterator:
                    if tweet['user']:
                        user_id = tweet['user']['id']
                        user_name = tweet['user']['name']
                        user_profile_img_url = tweet['user']['profile_image_url']
                        if tweet['entities']:
                            if self.hashtag in [x['text'] for x in tweet['entities']['hashtags']]:
                                self.queue.put((self.hashtag, user_id, user_name, user_profile_img_url))
                    if not self.do_run:
                        break
            except TwitterHTTPError as ex:
                logging.error(ex)
                time.sleep(30)
                continue
            except HTTPError as ex:
                logging.error(ex)
                break
        logging.info("Finishing")

from datastore import MemcacheDS
if __name__ == '__main__':
    import ConfigParser
    
    
    config = ConfigParser.RawConfigParser()
    config.read("oauth.cfg")
    if not config.has_section('TwitterOAuth'):
        logging.error("No TwitterOAuth section in config file")
        exit(1)
    
    auth_config = {
                    'token': config.get('TwitterOAuth', 'token'),
                    'token_secret': config.get('TwitterOAuth', 'token_secret'),
                    'consumer_key': config.get('TwitterOAuth', 'consumer_key'),
                    'consumer_secret': config.get('TwitterOAuth', 'consumer_secret')
    }
    
    LOGGING_FORMAT = "%(asctime)s:%(levelname)s:%(threadName)s:%(message)s"
    logging.basicConfig(level=logging.DEBUG, format=LOGGING_FORMAT)

    dataStore = MemcacheDS.MemcacheDS()
    
    msgQ = Queue()
    msgQh = QueueHandler(msgQ, dataStore)
    msgQh.start()
    
    jobTh = HashTagThread('job', msgQ, auth_config)
    jobTh.start()
    
    try:
        while msgQh.isAlive():
            msgQh.join(10)
    except KeyboardInterrupt:
        jobTh.do_run = False
        jobTh.join()
        msgQh.do_run = False
        msgQh.join()
        