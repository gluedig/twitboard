'''
Created on Nov 30, 2012

@author: raber
'''
from flask import Flask, abort, render_template
import memcache
from hashlib import md5
import json


app = Flask(__name__)

@app.route('/')
def main_route():
    return "hello"

@app.route('/api/tag/<hashtag>')
@app.route('/api/tag/<hashtag>/<int:number>')
def hashtag_topn(hashtag, number=10):
    
    hashtag_score_key = md5("hashtag:%s"%hashtag).hexdigest()
    hashtag_score = app.mc.get(hashtag_score_key)
    leaderboard = {'hashtag':hashtag, 'top':number, 'count': 0, 'list': []}
    
    count = 0
    if not hashtag_score:
        abort(404, "Hashtag not found")
        
    for (user_key, score) in hashtag_score.most_common(number):
        user_data = app.mc.get(user_key)
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
    user_hashtag_key = md5("user_id:%s,hashtag:%s"%(user_id,hashtag)).hexdigest()
    
    score = app.mc.get(user_hashtag_key)
    if not score:
        score = 0
    
    return json.dumps({'user_id': user_id, 'hashtag':hashtag, 'score':score})

@app.route('/api/user/<user_id>')
def user_info(user_id):
    user_key = md5("user_id:%s"%user_id).hexdigest()
    user_data = app.mc.get(user_key)
    
    if not user_data:
        abort(404, "User not fund")
    
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

if __name__ == '__main__':
    app.debug = True
    app.mc = memcache.Client(['127.0.0.1:11211'], debug=0)
    
    app.run()