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
import boto3

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
        
        # key 세팅
        service_name = config.service_name
        access_key = config.access_key
        secret_key = config.secret_key
        region_name = config.region_name

        # s3 클라이언트 생성
        s3 = boto3.client(service_name,
                            aws_access_key_id = access_key,
                            aws_secret_access_key = secret_key,
                            region_name = region_name
                            )

        # 업로드 버킷 위치
        bucket_name = config.bucket_name
        
        data_path = os.path.join(os.path.split(script_abs_path)[0], 'data')
        
        artists_json = os.path.join(data_path, 'artists.json')
        tracks_json = os.path.join(data_path, 'tracks.json')
        audio_features_json = os.path.join(data_path, 'audio_features.json')

        # 파일 업로드
        s3.upload_file(artists_json, bucket_name, 'artists/{}'.format(os.path.split(artists_json)[1]))
        s3.upload_file(tracks_json, bucket_name, 'tracks/{}'.format(os.path.split(tracks_json)[1]))
        s3.upload_file(audio_features_json, bucket_name, 'audio/{}'.format(os.path.split(audio_features_json)[1]))
        
        # 업로드 확인
        response = s3.list_objects_v2(Bucket = bucket_name)
        
        for i in range(len(response['Contents'])):
            print(response['Contents'][i]['Key'])
    except:
        logging.error('############ Main Funtion Error')
        logging.error(traceback.format_exc())
    
''' functions '''
        
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