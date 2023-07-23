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
import config
from sqlalchemy import create_engine

from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler

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
        
        # DB 정보 호출
        db_info = config.db_info
        
        # DATABASE 엔진 생성
        engine = create_db_engine(db_info)
        
        audio_feature_df = select_table(engine, 'music_audio')
        
        model = KMeans(n_clusters = int(len(audio_feature_df) / 6) + 1, random_state = 0)
        
        train_df = audio_feature_df.drop(['track_id', 'created_date'], axis = 1)
        
        train_df = MinMaxScaler().fit_transform(train_df)
        
        model.fit(train_df)
        
        audio_feature_df['cluster'] = model.fit_predict(train_df)
        
        audio_feature_df = audio_feature_df[['cluster', 'track_id']].sort_values('cluster').reset_index(drop = True)
        
        audio_feature_df.index = audio_feature_df.index + 1
        
        audio_feature_df = audio_feature_df.reset_index().rename({'index' : 'id'}, axis = 1)
        
        insert_to_table(engine, 'music_recommend', audio_feature_df)
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
            if_exists = 'replace',
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