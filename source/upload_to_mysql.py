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

import pandas as pd
import json

import config
from sqlalchemy import create_engine

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
        
        data_path = os.path.join(os.path.split(script_abs_path)[0], 'data')
        
        # DB 정보 호출
        db_info = config.db_info
        
        # DATABASE 엔진 생성
        engine = create_db_engine(db_info)
        
        # artists
        with open(os.path.join(data_path, 'artists.json'), encoding = 'UTF-8') as file:
            artists_json = json.load(file)
        
        # artist 데이터프레임 세팅
        artists_df = pd.DataFrame([], index = range(len(artists_json)), columns = ['artist_id', 'artist_name', 'artist_popularity', 'artist_followers', 'artist_image_link', 'created_date'])
        for idx, (key, val) in enumerate(artists_json.items()):
            artists_df.loc[idx, 'artist_id'] = key
            artists_df.loc[idx, 'artist_name'] = val['name']
            artists_df.loc[idx, 'artist_popularity'] = val['popularity']
            artists_df.loc[idx, 'artist_followers'] = val['followers']['total']
            if len(val['images']) != 0:
                artists_df.loc[idx, 'artist_image_link'] = val['images'][0]['url']
            artists_df.loc[idx, 'created_date'] = str(datetime.datetime.now())
        
        # tracks
        with open(os.path.join(data_path, 'tracks.json'), encoding = 'UTF-8') as file:
            tracks_json = json.load(file)
        
        tracks_df = pd.DataFrame([], index = range(len(tracks_json)), columns = ['track_id', 'track_name', 'track_popularity', 'track_image_link', 'track_release_date', 'created_date', 'artist_id', 'track_uri'])
        for idx, (key, val) in enumerate(tracks_json.items()):
            tracks_df.loc[idx, 'track_id'] = key
            tracks_df.loc[idx, 'track_name'] = val['name']
            tracks_df.loc[idx, 'track_popularity'] = val['popularity']
            if len(val['album']['images']) != 0:
                tracks_df.loc[idx, 'track_image_link'] = val['album']['images'][0]['url']
            tracks_df.loc[idx, 'track_release_date'] = val['album']['release_date']
            tracks_df.loc[idx, 'created_date'] = str(datetime.datetime.now())
            tracks_df.loc[idx, 'artist_id'] = val['artists'][0]['id']
            tracks_df.loc[idx, 'track_uri'] = val['uri']
        
        # track 데이터프레임 세팅
        tracks_df2 = tracks_df[tracks_df['artist_id'].isin(artists_df['artist_id'])]
        
        # audio_features
        with open(os.path.join(data_path, 'audio_features.json'), encoding = 'UTF-8') as file:
            audio_features_json = json.load(file)
                
        # audio_features 데이터프레임 세팅
        audio_features_df = pd.DataFrame([], index = range(len(audio_features_json)), columns = ['track_id', 'danceability', 'energy', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'created_date'])
        for idx, (key, val) in enumerate(audio_features_json.items()):
            if val == None:
                continue
            audio_features_df.loc[idx, 'track_id'] = key
            audio_features_df.loc[idx, 'danceability'] = val['danceability']
            audio_features_df.loc[idx, 'energy'] = val['energy']
            audio_features_df.loc[idx, 'loudness'] = val['loudness']
            audio_features_df.loc[idx, 'mode'] = val['mode']
            audio_features_df.loc[idx, 'speechiness'] = val['speechiness']
            audio_features_df.loc[idx, 'acousticness'] = val['acousticness']
            audio_features_df.loc[idx, 'instrumentalness'] = val['instrumentalness']
            audio_features_df.loc[idx, 'liveness'] = val['liveness']
            audio_features_df.loc[idx, 'valence'] = val['valence']
            audio_features_df.loc[idx, 'tempo'] = val['tempo']
            audio_features_df.loc[idx, 'created_date'] = str(datetime.datetime.now())
        
        # audio_feature 미존재 track 제외
        audio_features_df2 = audio_features_df[audio_features_df['track_id'].isin(tracks_df2['track_id'])]
        tracks_df3 = tracks_df2[tracks_df2['track_id'].isin(audio_features_df2['track_id'])]
        
        # 기존 Table 호출
        music_artist = select_table(engine, 'music_artist')
        music_track = select_table(engine, 'music_track')
        music_audio = select_table(engine, 'music_audio')
        
        # 중복 ID 제외
        artists_df2 = artists_df[~artists_df['artist_id'].isin(music_artist['artist_id'])]
        tracks_df4 = tracks_df3[~tracks_df3['track_id'].isin(music_track['track_id'])]
        audio_features_df3 = audio_features_df2[~audio_features_df2['track_id'].isin(music_audio['track_id'])]
        
        # 테이블 Insert
        insert_to_table(engine, 'music_artist', artists_df2)
        insert_to_table(engine, 'music_track', tracks_df4)
        insert_to_table(engine, 'music_audio', audio_features_df3)
    except:
        logging.error('############ Main Funtion Error')
        logging.error(traceback.format_exc())
    
''' functions '''
# DATABASE 엔진 생성
def create_db_engine(db_info):
    try:
        logging.info('#### Create Database Engine \"{}\"'.format(db_info))
        return create_engine(db_info)
    except RuntimeWarning as w:
        logging.warning(w)
    except:
        logging.error('############ Create Database Engine Error')
        logging.error(traceback.format_exc())

# DATABASE SELECT * FROM 테이블
def select_table(engine, table_name):
    try:
        logging.info('#### Select Table : \"{}\"'.format(table_name))
        df = pd.read_sql(
            sql = 'select * from {}'.format(table_name),
            con = engine,
        )

        return df
    except RuntimeWarning as w:
        logging.warning(w)
    except:
        logging.error('############ Read Table \"{}\" Error'.format(table_name))
        logging.error(traceback.format_exc())

# DATABASE INSERT TO TABLE 테이블
def insert_to_table(engine, table_name, df):
    try:
        logging.info('#### Insert To Table \"{}\"'.format(table_name))
        df.to_sql(
            name = table_name,
            con = engine,
            if_exists = 'append',
            index = False,
        )
    except RuntimeWarning as w:
        logging.warning(w)
    except:
        logging.error('############ Insert To Table \"{}\" Error'.format(table_name))
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