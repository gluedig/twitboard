'''
Created on Dec 1, 2012

@author: raber
'''
from collections import Counter, OrderedDict
from hashlib import md5
import memcache
import logging
import zmq

class OrderedCounter(Counter, OrderedDict):
    'Counter that remembers the order elements are first encountered'
    
    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, OrderedDict(self))
    
    def __reduce__(self):
        return self.__class__, (OrderedDict(self),)

class MemcacheDS(object):
    def __init__(self, flush=False, limit=25, zmq_ctx=None):
        self.mc = memcache.Client(['127.0.0.1:11211'], debug=0)
        self.limit = limit
        if flush:
            self.mc.flush_all()
            
        if zmq_ctx:
            self.zmq_socket = zmq_ctx.socket(zmq.PUB)
            self.zmq_socket.bind('inproc://queue') 
             

    def _user_key(self, user_id):
        return md5("user_id:%s"%user_id).hexdigest()
        
    def _hashtag_score_key(self, hashtag):
        return md5("hashtag:%s"%hashtag).hexdigest()
        
    def _user_hashtag_score_key(self, user_id, hashtag):
        return md5("user_id:%s,hashtag:%s"%(user_id,hashtag)).hexdigest()
    
    def _hashtag_users_key(self, hashtag):
        return md5("hashtagusers:%s"%hashtag).hexdigest()
    
    def hashtag_topn(self, hashtag, number):
        hashtag_score_key = self._hashtag_score_key(hashtag)
        hashtag_score = self.mc.get(hashtag_score_key)
        if hashtag_score:
            return hashtag_score.most_common(number)
        else:
            return None

    def user_data_byid(self, user_id):
        user_key = self._user_key(user_id)
        return self.user_data_bykey(user_key)
        
    def user_data_bykey(self, user_key):
        return self.mc.get(user_key)

    def user_score_byid(self, user_id, hashtag):
        user_hashtag_key = self._user_hashtag_score_key(user_id, hashtag)
        return self.mc.get(user_hashtag_key)

    def insert(self, hashtag, user_id, user_name, user_profile_img_url):
        user_key = self._user_key(user_id)
        user_hashtag_key = self._user_hashtag_score_key(user_id, hashtag) 
        hashtag_score_key = self._hashtag_score_key(hashtag)
        hashtag_users_key = self._hashtag_users_key(hashtag)
        
        #add/update user data        
        user_data = self.mc.get(user_key)
        if not user_data:
            user_data = (user_id, user_name, user_profile_img_url, [hashtag])
        
        (user_id, user_name, user_profile_img_url, user_hashtags) = user_data
        if hashtag not in user_hashtags:
            user_hashtags.append(hashtag)
        self.mc.set(user_key, user_data)
        
        #add/update users for hashtag
        hashtag_users = self.mc.get(hashtag_users_key)
        if not hashtag_users:
            hashtag_users = set()
        hashtag_users.add(user_id)
        self.mc.set(hashtag_users_key, hashtag_users)
        
        #add/update user hashtag score
        user_hashtag = self.mc.get(user_hashtag_key)
        if not user_hashtag:
            self.mc.set(user_hashtag_key, 0)
        score = self.mc.incr(user_hashtag_key)
        
        #add/update overall hastag score
        hashtag_score = self.mc.get(hashtag_score_key)
        if not hashtag_score:
            hashtag_score = OrderedCounter()
        
        score_list = [pair[0] for pair in sorted(hashtag_score.items(), key=lambda item: item[1], reverse=True)]
        old_index = -1
        if user_key in score_list:
            old_index = score_list.index(user_key)
        
        hashtag_score[user_key] = score
        if len(hashtag_score) > self.limit:
            hashtag_score = OrderedCounter(dict(hashtag_score.most_common(self.limit)))
       
        score_list = [pair[0] for pair in sorted(hashtag_score.items(), key=lambda item: item[1], reverse=True)]
        new_index = -1
        if user_key in score_list:
            new_index = score_list.index(user_key)
        
        self.mc.set(hashtag_score_key, hashtag_score)
        
        if self.zmq_socket and (new_index != -1 or old_index != -1):
            self.zmq_socket.send_unicode("%s;%s;%s;%d;%d;%d"%(hashtag, user_id, user_name, score, old_index, new_index))
            logging.debug("Hashtag: %s User: %s Score: %s Position old: %d new: %d", hashtag, user_id, score, old_index, new_index)
        
        #logging.debug("Hashtag: %s Top: %s", hashtag, hashtag_score)
        