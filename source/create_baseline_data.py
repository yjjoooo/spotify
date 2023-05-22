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
import json

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

        artist_list = pym.read_file_list(os.path.join(data_path, '01_artists'))
        track_list = pym.read_file_list(os.path.join(data_path, '02_tracks'))
        audio_feature_list = pym.read_file_list(os.path.join(data_path, '03_audio_features'))
        
        artists_json = concat_json(artist_list)
        logging.info('#### Save {}'.format(os.path.join(data_path, 'artists.json')))
        with open(os.path.join(data_path, 'artists.json'), 'w', encoding='UTF-8') as file:
            json.dump(artists_json, file, indent = 4, ensure_ascii = False)
        
        tracks_json = concat_json(track_list)
        logging.info('#### Save {}'.format(os.path.join(data_path, 'tracks.json')))
        with open(os.path.join(data_path, 'tracks.json'), 'w', encoding='UTF-8') as file:
            json.dump(tracks_json, file, indent = 4)

        audio_features_json = concat_json(audio_feature_list)
        logging.info('#### Save {}'.format(os.path.join(data_path, 'audio_features.json')))
        with open(os.path.join(data_path, 'audio_features.json'), 'w', encoding='UTF-8') as file:
            json.dump(audio_features_json, file, indent = 4)
    except:
        logging.error('############ Main Funtion Error')
        logging.error(traceback.format_exc())
    
''' functions '''
def concat_json(file_list):
    try:
        logging.info('#### Concat Json')
        result = {}
        
        for file_path in file_list:
            try:
                file_id = os.path.splitext(os.path.split(file_path)[-1])[0]
                with open(file_path, 'r') as file:
                    file_json = {file_id : json.load(file)}
                result.update(file_json)
            except:
                logging.error('######## Fail To Concat {}'.format(file_path))
                logging.error(traceback.format_exc())
        
        return result
    except:
        logging.error('############ Concat Json Error')
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