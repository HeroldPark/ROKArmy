import io
import csv
import time
import redis
import pandas as pd
import numpy as np
import pickle
import splunklib.client as client

from time import localtime, sleep
from direct_redis import DirectRedis
from rediscluster import RedisCluster 
# from prefect import flow, task, get_run_logger


# Redis 서버 정보 설정
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
redis_db = 0               # Redis 데이터베이스 
# splunk_index = 'roka_utm2'

# Redis 연결
# r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=redis_db) 

# # Direct Redis 연결
# direct_r = DirectRedis(host=REDIS_HOST, port=REDIS_PORT, db=redis_db)

# Define the Redis cluster nodes and configuration
startup_nodes = [
    {"host": REDIS_HOST, "port": "7000"},
    {"host": REDIS_HOST, "port": "7001"},
    {"host": REDIS_HOST, "port": "7002"},
    # Add more nodes if necessary
]

# RedisCluster configuration options
redis_config = {
    "startup_nodes": startup_nodes,
    "decode_responses": False,  # Set it to True if you want to receive decoded responses
}

# # Create a Redis cluster object
redis_cluster = RedisCluster(**redis_config)

# Splunk 서버 정보 설정
SPLUNK_HOST = '10.0.0.3'
SPLUNK_PORT = 8089
SPLUNK_USERNAME = 'admin'
SPLUNK_PASSWORD = 'demodemo' 

# Splunk 연결
service = client.connect(
    host=SPLUNK_HOST,
    port=SPLUNK_PORT,
    username=SPLUNK_USERNAME,
    password=SPLUNK_PASSWORD
) 

# Splunk 로그 가져오기
# @flow
def get_splunk_logs():
    current_time = time.time()
    minutes_ago = current_time - (1900 * 60)

    # # Run a Splunk search query
    search_query = f'search index="_internal" earliest={minutes_ago} latest=now()'
    # search_query = 'search source=tbl_session3.csv'
    search_kwargs = {'output_mode': 'csv'}

    tm = localtime(minutes_ago)
    print("start search: ", time.strftime("%d-%m-%Y %H:%M:%S", tm))
    search_results = service.jobs.export(search_query, **search_kwargs)   

    mid_time = time.time()
    print("start read: %s" % search_query)
    # # Open the search results as a file-like object using io.BytesIO
    logs = io.BytesIO(search_results.read())
    # DF = pd.read_csv(search_results, engine='pyarrow')

    fin_time = time.time()
    print(f"search_time: {mid_time-current_time}, read_time: {fin_time-mid_time}")
    
    return logs


# Redis에 로그 저장
# @flow
def save_logs_to_redis(logs, splunk_index):

    # # 문자열을 encode하면 byte형이 되고 byte형을 decode하면 문자열이 됩니다.
    # data = []
    # st = time.time()    
    # for log in logs:
    #     # if counter < 4:
    #         # log = log[-1]
    #     log = log.decode('utf-8')
        
    #     log = log.split(',')
    #     log = log[7:]
    #     log = list(map(lambda x: x.replace('"', ''), log))
    #     data.append(log)
    
    # logs.close()
    # columns = ['session_id', 'start_time', 'end_time', 'src_ip', 'src_port', 'dst_ip', 'dst_port', 'proto', 'state', 'duration', 'sent_pkt', 'rcvd_pkt', 'sent_byte', 'rcvd_byte']

    # df = pd.DataFrame(data, columns=columns) 
    # df.drop([0], axis=0, inplace=True)
    # df = df.astype({'session_id':int, 'duration':float, 'sent_pkt':int, 'rcvd_pkt':int, 'sent_byte':int, 'rcvd_byte':int})
    # et = time.time()
    # print(f'data_fame_time : {et - st}')
    # redis_st = time.time()
    # redis_cluster.set(splunk_index, df)
    # redis_md = time.time()
    # print(f'redis_set_time : {redis_md - redis_st}')
    # redis_cluster.save()    # Redis 데이터 저장 
    # redis_et = time.time()
    # print(f'redis_save_time : {redis_et - redis_md}')

    # Serialize the io.BytesIO object using pickle
    # serialized_data = pickle.dumps(logs)

    redis_st = time.time()
    for log in logs:
        # if counter < 4:
            # log = log[-1]
        log = log.decode('utf-8')
        redis_cluster.rpush(splunk_index, log)


    # redis_cluster.lpush(splunk_index, data)
    redis_md = time.time()
    print(f'redis_set_time : {redis_md - redis_st}')
    redis_cluster.save()    # Redis 데이터 저장 
    redis_et = time.time()
    print(f'redis_save_time : {redis_et - redis_md}')

# 주기적으로 로그 저장 및 불러오기
# @flow(name="Save log from Splunk to Redis")
def run():    
    splunk_index = 0      

    # while True:
        
    # Increment the key
    splunk_index += 1

    # logger = get_run_logger()
    
    logs = get_splunk_logs()
    # logger.info('Data fetch done')

    save_logs_to_redis(logs, f'roka_utm{splunk_index}')
    # logger.info('Process End')

        # Wait for one minute
        # time.sleep(60)

# 실행
if __name__ == '__main__':
    run()
    service.logout()