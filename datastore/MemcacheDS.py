'''
Created on Dec 1, 2012

@author: raber
'''
from collections import Counter
from hashlib import md5
import memcache
import logging

class MemcacheDS(object):
    def __init__(self, flush=False, limit=25):
        self.mc = memcache.Client(['127.0.0.1:11211'], debug=0)
        self.limit = limit
        if flush:
            self.mc.flush_all()

    def _user_key(self, user_id):
        return md5("user_id:%s"%user_id).hexdigest()
        
    def _hashtag_score_key(self, hashtag):
        return md5("hashtag:%s"%hashtag).hexdigest()
        
    def _user_hashtag_score_key(self, user_id, hashtag):
        return md5("user_id:%s,hashtag:%s"%(user_id,hashtag)).hexdigest()
    
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
        if len(hashtag_score) > self.limit:
            hashtag_score = Counter(dict(hashtag_score.most_common(self.limit)))
            
        
        self.mc.set(hashtag_score_key, hashtag_score)
        #logging.debug("Hashtag: %s Top: %s", hashtag, hashtag_score)