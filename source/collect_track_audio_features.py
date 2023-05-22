#!/usr/bin/python
# -*- coding : utf-8 -*-
'''
 @author : yjjo
'''
''' install '''

''' import '''
import python_modules as pym
import logging
from logging.config import dictConfig
import datetime
import traceback
import os
import config
import json
import requests
import base64
import time

''' log '''
script_abs_path, script_name = os.path.split(__file__)

pym.create_dir(os.path.join(script_abs_path, 'logs'))

dictConfig({
    'version' : 1,
    'formatters' : {
        'default' : {
            'format' : '[%(asctime)s] %(levelname)7s --- %(lineno)6d : %(message)s',
        },
    },
    'handlers' : {
        'file' : {
            'class' : 'logging.FileHandler',
            'level' : 'DEBUG',
            'formatter' : 'default',
            'filename' : os.path.join(script_abs_path, 'logs', '{}_{}.log'.format(script_name.rsplit('.', maxsplit = 1)[0], datetime.datetime.now().strftime('%Y%m%d'))),
        },
    },
    'root' : {
        'level' : 'DEBUG',
        'handlers' : ['file']
    }
})

''' main function'''
def main():
    try:
        pym.script_start()

        # 스포티파이 client 정보 세팅
        client_id = config.spotify_client_id
        client_secret = config.spotify_client_secret
        
        # artist 목록 조회
        artist_list = pym.read_file_list(os.path.join(script_abs_path, '..', 'data', '01_artists'))
        artist_list = list(map(lambda x : os.path.splitext(os.path.split(x)[-1])[0], artist_list))
        
        for artist_id in artist_list:
            try:
                # artist hot track 호출
                hot_tracks = collect_artist_hot_track(client_id, client_secret, artist_id, 'KR')
                
                for track in hot_tracks['tracks']:
                    track_id = track['id']
                    track_name = track['name']
                    
                    try:
                        # 저장
                        logging.info('######## Save Track {} / {}'.format(track_id, track_name))
                        with open(os.path.join(script_abs_path, '..', 'data', '04_tracks', '{}.json'.format(track_id)), 'w') as file:
                            json.dump(track, file, indent = 4)
                    except:
                        logging.error('######## Save Track Error {} / {}'.format(track_id, track_name))
                    
                    try:
                        # audio feature 호출
                        response = collect_audio_features(client_id, client_secret, track_id, 'KR')
                        
                        # 저장
                        logging.info('######## Save Audio Features {} / {}'.format(track_id, track_name))
                        with open(os.path.join(script_abs_path, '..', 'data', '05_audio_features', '{}.json'.format(track_id)), 'w') as file:
                            json.dump(response, file, indent = 4)
                    except:
                        logging.error('######## Save Audio Features Error {} / {}'.format(track_id, track_name))
            except:
                pass
    except:
        logging.error('############ Main Funtion Error')
        logging.error(traceback.format_exc())
    
''' functions '''
# 스포티파이 API 인증
def get_headers(client_id, client_secret):
    try:
        b64 = base64.b64encode('{}:{}'.format(client_id, client_secret).encode('UTF-8')).decode('ascii')
        headers = {
            'Authorization' : 'Basic {}'.format(b64)
        }
        
        form = {
            'grant_type' : 'client_credentials'
        }
        
        response = requests.post('https://accounts.spotify.com/api/token', headers = headers, data = form)
        
        token = response.json()['access_token']
        
        headers = {
            'Authorization' : 'Bearer {}'.format(token)
        }
        
        return headers
    except Warning as w:
        logging.warning(w)
    except:
        logging.error('############ Get Headers Error')
        logging.error(traceback.format_exc())
        return get_headers(client_id, client_secret)

# artist hot track API 호출
def collect_artist_hot_track(client_id, client_secret, artist_id, market):
    try:
        headers = get_headers(client_id, client_secret)
        
        params = {
            'market' : market
        }
        
        response = requests.get('https://api.spotify.com/v1/artists/{}/top-tracks'.format(artist_id), headers = headers, params = params)
        
        if response.status_code == 200:
            logging.info('#### Success Collect Artist Hot Track {} / {}'.format(artist_id, market))
            return response.json()
        elif response.status_code == 401:
            logging.error('############ Unauthorized Error Collect Artist Hot Track {} / {}'.format(artist_id, market))
            return collect_artist_hot_track(client_id, client_secret, artist_id, market)
        elif response.status_code == 429:
            retry_after = response.headers['Retry-After']
            logging.error('############ Too Many Requests Error Collect Artist Hot Track {} / {} / Time : {}'.format(artist_id, market, retry_after))
            time.sleep(int(retry_after))
            return collect_artist_hot_track(client_id, client_secret, artist_id, market)
        else:
            logging.error('############ Collect Artist Hot Track API Error {} / {} / {}'.format(response.status_code, artist_id, market))
    except Warning as w:
        logging.warning(w)
    except:
        logging.error('############ Collect Artist Hot Track API Error')
        logging.error(traceback.format_exc())
        return collect_artist_hot_track(client_id, client_secret, artist_id, market)

# artist audio features API 호출
def collect_audio_features(client_id, client_secret, track_id, market):
    try:
        headers = get_headers(client_id, client_secret)
        
        params = {
            'market' : market
        }
        
        response = requests.get('https://api.spotify.com/v1/audio-features/{}'.format(track_id), headers = headers, params = params)
        if response.status_code == 200:
            logging.info('#### Success Collect Audio Features {} / {}'.format(track_id, market))
            return response.json()
        elif response.status_code == 401:
            logging.error('#### Unauthorized Error Collect Audio Features {} / {}'.format(track_id, market))
            return collect_audio_features(client_id, client_secret, track_id, market)
        elif response.status_code == 429:
            retry_after = response.headers['Retry-After']
            logging.error('#### Too Many Requests Error Collect Audio Features {} / {} / Time : {}'.format(track_id, market, retry_after))
            time.sleep(int(retry_after))
            return collect_audio_features(client_id, client_secret, track_id, market)
        else:
            logging.error('#### Collect Audio Features API Error {} / {} / {}'.format(response.status_code, track_id, market))
    except Warning as w:
        logging.warning(w)
    except:
        logging.error('############ Collect Audio Features API Error')
        logging.error(traceback.format_exc())
        return collect_audio_features(client_id, client_secret, track_id, market)

''' main '''
if __name__ == '__main__':
    # 런타임
    start_time = datetime.datetime.now()

    ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

    main()

    ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    
    logging.info('==========================================================================================')
    logging.info('#### Run Time {}'.format(str(datetime.datetime.now() - start_time)))
    logging.info('==========================================================================================')