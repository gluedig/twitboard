'''
Created on Nov 30, 2012

@author: raber
'''
from flask import Flask, abort, render_template, Response, request
import json
from datastore import MemcacheDS
import zmq

app = Flask(__name__)
app.debug = True
app.zmq_ctx = zmq.Context() 
app.data = MemcacheDS.MemcacheDS()

@app.route('/')
def main_route():
    return "hello"

@app.route('/api/tag/<hashtag>')
def hashtag_topn(hashtag):
    
    hashtag_score = app.data.hashtag_topn(hashtag)
    leaderboard = {'hashtag':hashtag, 'count': 0, 'list': []}
    
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
def page_hashtag_topn(hashtag):
    animate = 'false'
    results = 25
    if 'animate' in request.args:
        animate = request.args['animate']
    if 'results' in request.args:
        try:
            results = int(request.args['results'])
            if results > 25:
                results = 25
        except ValueError:
            pass
            
    return render_template('hashtag_score.tmp', hashtag=hashtag,  animate=animate, results=results)

from collections import Iterable
class HashtagUpdates(Iterable):
    def __init__(self, hashtag, ds, zmq_ctx=None):
        self.hashtag = hashtag
        self.data = ds
        self.n = 0
        if zmq_ctx:
            self.zmq_socket = zmq_ctx.socket(zmq.SUB)
            self.zmq_socket.setsockopt(zmq.SUBSCRIBE, '')
            self.zmq_socket.connect("tcp://localhost:5556") 
             
        
    def __iter__(self):
        while True:
            if self.zmq_socket:
                string = self.zmq_socket.recv_unicode()
                (hashtag, user_id, user_name, score, old_pos, new_pos) = string.split(";")
                
                if hashtag == self.hashtag:    
                    data = {'msgtype': 'update', 'user_id': user_id, 'old_pos' : old_pos, 'new_pos': new_pos, 'score': score, 'user_name': user_name}
                    self.n += 1
                    yield unicode("event: message\nid: {0}\ndata: {1}\n\n".format(self.n, json.dumps(data)))


@app.route('/updates/<hashtag>', methods=["POST","GET"])
def hashtag_updates(hashtag):
    return Response(HashtagUpdates(hashtag, ds=app.data, zmq_ctx=app.zmq_ctx), headers=[('cache-control','no-cache'), ('connection', 'keep-alive')],
        content_type='text/event-stream')

if __name__ == '__main__':
    app.run(host='0.0.0.0', threaded=True)