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
import pandas as pd
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

        client_id = config.spotify_client_id
        client_secret = config.spotify_client_secret
                
        artist_df = pd.read_csv(os.path.join(script_abs_path, 'data', 'artists.csv'))
        artist_df['artist_lastfm'] = artist_df['artist_lastfm'].fillna('etc')
        artist_df['country_mb'] = artist_df['country_mb'].fillna('etc')
        
        artist_df_korean = artist_df[artist_df['country_mb'] == 'South Korea'].reset_index(drop = True)
        
        artist_df2 = artist_df['country_mb'].value_counts().reset_index()
        
        artist_df3 = artist_df[artist_df['country_mb'].isin(list(artist_df2[artist_df2['country_mb'] > 5000]['index']))][artist_df['listeners_lastfm'] > 0][artist_df['scrobbles_lastfm'] > 0]
        artist_df3 = artist_df3.drop_duplicates(subset = ['artist_lastfm']).reset_index(drop = True)
        
        artist_df4 = artist_df3.loc[:15000, :]
        
        artist_df5 = pd.concat([artist_df4, artist_df_korean], axis = 0).drop_duplicates().reset_index(drop = True)
        
        artist_df5['artist_lastfm'] = artist_df5.apply(lambda x : x['artist_mb'] if x['artist_lastfm'] == 'etc' else x['artist_lastfm'] , axis = 1)
        
        save_path = os.path.join(script_abs_path, 'data', '01_artists')
            
        for artist_name in list(artist_df5['artist_lastfm']):
            response = search(client_id, client_secret, artist_name, 'artist', 'KR', 50, 0)
            
            save_artist(response, artist_name, save_path)
    except:
        logging.error('############ Main Funtion Error')
        logging.error(traceback.format_exc())
    
''' functions '''
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
    
def search(client_id, client_secret, query, search_type, market, limit, offset):
    try:
        headers = get_headers(client_id, client_secret)
        
        params = {
            'q' : query,
            'type' : search_type,
            'market' : market,
            'limit' : limit,
            'offset' : offset,
        }
        
        response = requests.get('https://api.spotify.com/v1/search', headers = headers, params = params)
        
        if response.status_code == 200:
            logging.info('############ Success Search {} / {} / {} / {} / {}'.format(query, search_type, market, limit, offset))
            return response.json()
        elif response.status_code == 401:
            logging.error('############ Unauthorized Error Search {} / {} / {} / {} / {}'.format(query, search_type, market, limit, offset))
            return search(client_id, client_secret, query, search_type, market, limit, offset)
        elif response.status_code == 429:
            retry_after = response.headers['Retry-After']
            logging.error('############ Too Many Requests Error Search {} / {} / {} / {} / {} / Time : {}'.format(query, search_type, market, limit, offset, retry_after))
            time.sleep(int(retry_after))
            return search(client_id, client_secret, query, search_type, market, limit, offset)
        else:
            logging.error('############ Search API Error {} / {} / {} / {} / {} / {}'.format(response.status_code, query, search_type, market, limit, offset))
    except Warning as w:
        logging.warning(w)
    except:
        logging.error('############ Search API Error')
        logging.error(traceback.format_exc())
        return search(client_id, client_secret, query, search_type, market, limit, offset)

def save_artist(response, artist_name, save_path):
    try:
        artist_list = response['artists']['items']
        
        artist_list = [x for x in artist_list if x['name'] == artist_name]
        
        for i in range(len(artist_list)):
            artist_id = artist_list[i]['id']
            logging.info('######## Save Artist {} / {}'.format(artist_id, artist_name))
            with open(os.path.join(save_path, '{}.json'.format(artist_id)), 'w') as file:
                json.dump(artist_list[i], file, indent = 4)
    except Warning as w:
        logging.warning(w)
    except:
        logging.error('############ Save Artist JSON Error')
        logging.error(traceback.format_exc())

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