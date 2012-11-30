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

from collections import Counter
from hashlib import md5
import memcache

SCORE_LIMIT = 25
class MemcDataStore(object):
    def __init__(self, flush=False):
        self.mc = memcache.Client(['127.0.0.1:11211'], debug=0)
        if flush:
            self.mc.flush_all()

    def insert(self, hashtag, user_id, user_name, user_profile_img_url):
        user_key = md5("user_id:%s"%user_id).hexdigest()
        user_hashtag_key = md5("user_id:%s,hashtag:%s"%(user_id,hashtag)).hexdigest()
        hashtag_score_key = md5("hashtag:%s"%hashtag).hexdigest()
        
        
        user_data = self.mc.get(user_key)
        if not user_data:
            user_data = (user_id, user_name, user_profile_img_url, [hashtag])
        
        (user_id, user_name, user_profile_img_url, user_hashtags) = user_data
        if hashtag not in user_hashtags:
            user_hashtags.append(hashtag)
        
        self.mc.set(user_key, user_data)
        
        user_hashtag = self.mc.get(user_hashtag_key)
        if not user_hashtag:
            self.mc.set(user_hashtag_key, 0)
            
        score = self.mc.incr(user_hashtag_key)
        logging.debug("Hashtag: %s User: %s Score: %s", hashtag, user_id, score)
        
        hashtag_score = self.mc.get(hashtag_score_key)
        if not hashtag_score:
            hashtag_score = Counter()
        
        hashtag_score[user_key] = score
        if len(hashtag_score) > SCORE_LIMIT:
            hashtag_score = Counter(dict(hashtag_score.most_common(SCORE_LIMIT)))
                
        
        self.mc.set(hashtag_score_key, hashtag_score)
        #logging.debug("Hashtag: %s Top: %s", hashtag, hashtag_score)

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

    dataStore = MemcDataStore()
    
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
        