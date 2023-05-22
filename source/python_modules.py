import logging
import os
import traceback
import platform
import sys
import shutil
import pandas as pd

# 스크립트 시작
def script_start():
    logging.info('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
    logging.info('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
    logging.info('@@@@                                                                                  @@@@')
    logging.info('@@@@    ___            _     _                                      _     _           @@@@')
    logging.info('@@@@   / _ \          | |   | |                      _             (_)   (_)          @@@@')
    logging.info('@@@@  / /_\ \  _   _  | |_  | |__     ___    _ __   (_)   _   _     _     _    ___    @@@@')
    logging.info('@@@@  |  _  | | | | | | __| |  _ \   / _ \  |  __|       | | | |   | |   | |  / _ \   @@@@')
    logging.info('@@@@  | | | | | |_| | | |_  | | | | | (_) | | |      _   | |_| |   | |   | | | (_) |  @@@@')
    logging.info('@@@@  \_| |_/  \__ _|  \__| |_| |_|  \___/  |_|     (_)   \__  |   | |   | |  \___/   @@@@')
    logging.info('@@@@                                                       __/ |  _/ |  _/ |          @@@@')
    logging.info('@@@@                                                      |___/  |__/  |__/           @@@@')
    logging.info('@@@@                                                                                  @@@@')
    logging.info('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
    logging.info('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
    logging.info('==========================================================================================')
    logging.info('#### OS              : {}'.format(platform.platform()))
    logging.info('#### Python Version  : {}'.format(sys.version))
    logging.info('#### Process ID(PID) : {}'.format(os.getpid()))
    logging.info('==========================================================================================')

''' file & directory controller '''

# 파일 리스트 호출
def read_file_list(path):
    try:
        logging.info('#### Read Path \"{}\"'.format(path))
        file_list = list([])
        
        for dir_path, _, file_name in os.walk(path):
            for f in file_name:
                try:
                    file_list.append(os.path.abspath(os.path.join(dir_path, f)))
                except:
                    logging.info('######## Read File \"{}\" Error'.format(file_name))
                    logging.info(traceback.format_exc())

        logging.info('############ Path \"{}\" File Count : {}'.format(path, len(file_list)))
        return file_list
    except RuntimeWarning as w:
        logging.warning(w)
    except:
        logging.error('############ Read Path \"{}\" Error'.format(path))
        logging.error(traceback.format_exc())
        
# 디렉토리 생성
def create_dir(path):
    try:
        if not os.path.exists(path):
            logging.info('#### Create Directory \"{}\"'.format(path))
            os.makedirs(path)
        else:
            logging.info('#### Directory \"{}\" Already Exist'.format(path))
    except RuntimeWarning as w:
        logging.warning(w)
    except:
        logging.error('############ Create Directory \"{}\" Error'.format(path))
        logging.error(traceback.format_exc())
        
# 파일 복사 
def copy_file(file_from, file_to):
    try:
        logging.info('#### Copy File \"{}\" to \"{}\"'.format(file_from, file_to))
        if not os.path.exists(file_to):
            shutil.copy2(file_from, file_to)
        else:
            logging.info('######## File \"{}\" Already Exist'.format(file_to))
            file_to = '{}_copy.{}'.format(file_to.rsplit('.', maxsplit = 1)[0], file_to.rsplit('.', maxsplit = 1)[1])
            logging.info('######## Copy File \"{}\" to \"{}\"'.format(file_from, file_to))
    except RuntimeWarning as w:
        logging.warning(w)
    except:
        logging.error('############ Copy File \"{}\" to \"{}\" Error'.format(file_from, file_to))
        logging.error(traceback.format_exc())

# 폴더 복사
def copy_dir(dir_from, dir_to):
    try:
        logging.info('#### Copy Direcotry \"{}\" to \"{}\"'.format(dir_from, dir_to))
        if not os.path.exists(dir_to):
            shutil.copytree(dir_from, dir_to, dirs_exist_ok = True)
        else:
            logging.info('######## Direcotry \"{}\" Already Exist'.format(dir_to))
    except RuntimeWarning as w:
        logging.warning(w)
    except:
        logging.error('############ Copy Direcotry \"{}\" to \"{}\" Error'.format(dir_from, dir_to))
        logging.error(traceback.format_exc())

# 파일 or 폴더 이동
def move(fd_from, fd_to):
    try:
        logging.info('#### Move \"{}\" to \"{}\"'.format(fd_from, fd_to))
        if not os.path.exists(fd_to):
            shutil.move(fd_from, fd_to)
        else:
            logging.info('######## \"{}\" Already Exist'.format(fd_to))
            fd_to = '{}_copy.{}'.format(fd_to.rsplit('.', maxsplit = 1)[0], fd_to.rsplit('.', maxsplit = 1)[1])
            logging.info('######## Move \"{}\" to \"{}\"'.format(fd_from, fd_to))
    except RuntimeWarning as w:
        logging.warning(w)
    except:
        logging.error('############ Move \"{}\" to \"{}\" Error'.format(fd_from, fd_to))
        logging.error(traceback.format_exc())
        
# 파일 삭제
def remove_file(path):
    try:
        logging.info('#### Remove File \"{}\"'.format(path))
        if os.path.exists(path):
            os.remove(path)
        else:
            logging.info('######## No File \"{}\"'.format(path))
    except RuntimeWarning as w:
        logging.warning(w)
    except:
        logging.error('############ Remove File \"{}\" Error'.format(path))
        logging.error(traceback.format_exc())

# 폴더 삭제
def remove_dir(path):
    try:
        logging.info('#### Remove Directory \"{}\"'.format(path))
        if os.path.exists(path):
            shutil.rmtree(path)
        else:
            logging.info('######## No Directory \"{}\"'.format(path))
    except RuntimeWarning as w:
        logging.warning(w)
    except:
        logging.error('############ Remove Directory \"{}\" Error'.format(path))
        logging.error(traceback.format_exc())

# 파일 or 폴더 이름 변경
def rename(fd_from, fd_to):
    try:
        logging.info('#### Rename \"{}\" to \"{}\"'.format(fd_from, fd_to))
        if os.path.exists(fd_from):
            os.rename(fd_from, fd_to)
        else:
            logging.info('######## No \"{}\"'.format(fd_from))
    except RuntimeWarning as w:
        logging.warning(w)
    except:
        logging.error('############ Rename \"{}\" to \"{}\" Error'.format(fd_from, fd_to))
        logging.error(traceback.format_exc())

# 파일 사이즈 호출
def get_size_of_file(path_list):
    try:
        logging.info('#### Get File Size')
        
        # define dataframe
        df = pd.DataFrame([], columns = ['file_path', 'byte'])
        df['file_path'] = path_list
        
        for idx, path in enumerate(path_list):
            try:
                if os.path.exists(path):
                    file_size = os.path.getsize(path)
                    logging.info('######## #{} File \"{}\" Size : {} bytes'.format(idx, path, file_size))
                    df.loc[idx, 'byte'] = file_size
                else:
                    logging.info('######## #{} No File \"{}\"'.format(idx, path))
            except:
                logging.info('######## #{} Get File Size of \"{}\" Error'.format(idx, path))
                logging.info(traceback.format_exc())
        
        return df
    except RuntimeWarning as w:
        logging.warning(w)
    except:
        logging.error('############ Get File Size Error')
        logging.error(traceback.format_exc())