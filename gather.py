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
from datastore import MemcacheDS
import zmq


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
                #logging.debug("Hashtag: %s Id: %d Name: %s", hashtag, user_id, user_name)
                self.data.insert(hashtag, user_id, user_name, user_profile_img_url)
                self.queue.task_done()
            except Empty:
                continue
        logging.info("Finishing")
                

class HashTagThread(threading.Thread):
    
    def __init__(self, hashtags, msgQueue, auth_config):
        super(HashTagThread, self).__init__(name="HTT<%s>"%hashtags)
        self.hashtags = hashtags
        self.queue = msgQueue
        self.daemon = True
        self.do_run = True 
        self.auth_config = auth_config
        self.twitter_stream = None
    
    def run(self):
        logging.info("Starting, hashtag: %s", self.hashtags)
        while(self.do_run):
            try:
                logging.debug("Connecting & authenticating")
                self.twitter_stream = TwitterStream(auth=OAuth(
                    token=self.auth_config['token'],
                    token_secret=self.auth_config['token_secret'],
                    consumer_key=self.auth_config['consumer_key'],
                    consumer_secret=self.auth_config['consumer_secret']))
                
                logging.debug("Connecting filtered stream")
                iterator = self.twitter_stream.statuses.filter(track=','.join(self.hashtags))
                
                for tweet in iterator:
                    
                    try:
                        if 'user' in tweet:
                            user_id = tweet['user']['id']
                            user_name = tweet['user']['name']
                            user_profile_img_url = tweet['user']['profile_image_url']
                            if tweet['entities']:
                                for hashtag in self.hashtags:
                                    if hashtag in [x['text'] for x in tweet['entities']['hashtags']]:
                                        self.queue.put((hashtag, user_id, user_name, user_profile_img_url))
                        
                        elif 'limit' in tweet:
                            logging.warn("Rate limited, oustanding tweets %d", tweet['limit']['track'])
                        
                        elif 'disconnect' in tweet:
                            logging.critical("Stream disconnected code %d stream %s reason %s",
                                             tweet['disconnect']['code'], tweet['disconnect']['stream_name'], tweet['disconnect']['reason'])
                            self.do_run = False
                            
                            
                    except KeyError:
                        logging.error("Unknown: %s", tweet)
                        self.do_run = False
                        
                    
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

class ZmqRepeater(threading.Thread):
    def __init__(self, context):
        super(ZmqRepeater, self).__init__(name="ZMQRepeater")
        self.context = context
        self.daemon = True
        self.do_run = True 
        
        
    def run(self):
        '''Funnel messages coming from the external tcp socket to an inproc socket''' 
         
        sock_outgoing = self.context.socket(zmq.PUB) 
        sock_outgoing.bind('tcp://*:5556') 
        
        sock_incoming = self.context.socket(zmq.SUB)
        sock_incoming.setsockopt(zmq.SUBSCRIBE, "")
        sock_incoming.connect('inproc://queue') 
         
        while self.do_run: 
            msg = sock_incoming.recv() 
            sock_outgoing.send(msg) 


if __name__ == '__main__':
    import ConfigParser
    
    zmq_context = zmq.Context() 
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
    
    LOGGING_FORMAT = "%(asctime)s:%(levelname)s:%(threadName)s:%(funcName)s:%(message)s"
    logging.basicConfig(level=logging.DEBUG, format=LOGGING_FORMAT)

    dataStore = MemcacheDS.MemcacheDS(flush=True, limit=25, zmq_ctx=zmq_context)
    
    zmqReap = ZmqRepeater(zmq_context)
    zmqReap.start()
    
    msgQ = Queue()
    msgQh = QueueHandler(msgQ, dataStore)
    msgQh.start()
    
    jobTh1 = HashTagThread(['ipadgames', 'jobs'], msgQ, auth_config)
    jobTh1.start()
    
    
    try:
        while msgQh.isAlive():
            msgQh.join(10)
    except KeyboardInterrupt:
        jobTh1.do_run = False
        jobTh1.join()
    
        msgQh.do_run = False
        msgQh.join()
        