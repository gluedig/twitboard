'''
Created on Nov 30, 2012

@author: raber
'''
from flask import Flask, abort, render_template, Response, request
import json
import time

app = Flask(__name__)

@app.route('/')
def main_route():
    return "hello"

@app.route('/api/tag/<hashtag>')
@app.route('/api/tag/<hashtag>/<int:number>')
def hashtag_topn(hashtag, number=10):
    
    hashtag_score = app.data.hashtag_topn(hashtag, number)
    leaderboard = {'hashtag':hashtag, 'top':number, 'count': 0, 'list': []}
    
    count = 0
    if not hashtag_score:
        abort(404, "Hashtag not found")
        
    for (user_key, score) in hashtag_score:
        user_data = app.data.user_data_bykey(user_key)
        if not user_data:
            continue
        
        (user_id, user_name, user_profile_img_url, _user_hashtags) = user_data
        user_info = { 'user_id':user_id, 'user_name':user_name, 'user_profile_img_url':user_profile_img_url} 
        leaderboard['list'].append( {'score': score,'user_info': user_info}) 

        count += 1 

    
    leaderboard['count'] = count
    return json.dumps(leaderboard)

@app.route('/api/user/<user_id>/<hashtag>')
def user_hashtag_score(user_id, hashtag):
  
    score = app.data.user_score_byid(user_id, hashtag)
    if not score:
        score = 0
    
    return json.dumps({'user_id': user_id, 'hashtag':hashtag, 'score':score})

@app.route('/api/user/<user_id>')
def user_info(user_id):
    user_data = app.data.user_data_byid(user_id)
    
    if not user_data:
        abort(404, "User not found")
    
    (_user_id, user_name, user_profile_img_url, user_hashtags) = user_data
    
    return json.dumps({'user_id': user_id, 'hashtags':user_hashtags, 'user_name':user_name, 'user_profile_img_url':user_profile_img_url})


@app.route('/user/<user_id>/<hashtag>')
def page_user_hashtag_score(user_id, hashtag):
    return render_template('user_score.tmp', user_id=user_id, hashtag=hashtag)

@app.route('/user/<user_id>')
def page_user_info(user_id):
    return render_template('user_info.tmp', user_id=user_id)


@app.route('/tag/<hashtag>')
@app.route('/tag/<hashtag>/<int:number>')
def page_hashtag_topn(hashtag, number=10):
    return render_template('hashtag_score.tmp', hashtag=hashtag, number=number)

from collections import Iterable
import random
class HashtagUpdates(Iterable):
    def __init__(self, hashtag, ds):
        self.hashtag = hashtag
        self.data = ds
        self.n = 0
        
        
    def __iter__(self):
        hashtag_score = self.data.hashtag_topn(self.hashtag, 25)
        while hashtag_score:
            
            pos = random.randint(0, len(hashtag_score)-1)
            dir = -1 if random.randint(0,1) == 0 else 1
            dist = random.randint(1,len(hashtag_score)/3)
            move = dir*dist
            new_pos = pos + (dir*dist)
            
            if new_pos < 0 or new_pos > len(hashtag_score):
                new_pos = pos

            user_id = self.data.user_data_bykey(hashtag_score[pos][0])[0]
            score = hashtag_score[pos][1]
            data = {'user_id': user_id, 'move' : move, 'score': score}
            yield unicode("event: message\nid: {0}\ndata: {1}\n\n".format(self.n, json.dumps(data)))
            
            time.sleep(5)
            hashtag_score = self.data.hashtag_topn(self.hashtag, 25)
            self.n += 1

@app.route('/updates/<hashtag>', methods=["POST","GET"])
def hashtag_updates(hashtag):
    return Response(HashtagUpdates(hashtag, app.data), headers=[('cache-control','no-cache'), ('connection', 'keep-alive')],
        content_type='text/event-stream')

from datastore import MemcacheDS
if __name__ == '__main__':
    app.debug = True
    app.data = MemcacheDS.MemcacheDS()
    app.run(threaded=True)