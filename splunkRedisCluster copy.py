from datetime import datetime
import io
import time
import redis
import numpy as np
import pickle
import splunklib.client as client
from time import localtime, sleep
from direct_redis import DirectRedis
import pandas as pd
from io import StringIO
from rediscluster import RedisCluster 

# Define the Redis cluster nodes and configuration
startup_nodes = [
    {"host": "localhost", "port": "7000"},
    {"host": "localhost", "port": "7001"},
    {"host": "localhost", "port": "7002"},
    {"host": "localhost", "port": "7003"},
    {"host": "localhost", "port": "7004"},
    {"host": "localhost", "port": "7005"},
    # Add more nodes if necessary
]

# RedisCluster configuration options
redis_config = {
    "startup_nodes": startup_nodes,
    "decode_responses": False,  # Set it to True if you want to receive decoded responses
}

# Create a Redis cluster object
redis_cluster = RedisCluster(**redis_config)

# Splunk 서버 정보 설정
# SPLUNK_HOST = '10.0.0.3'
# SPLUNK_PORT = 8089
# SPLUNK_USERNAME = 'admin'
# SPLUNK_PASSWORD = 'demodemo' 
SPLUNK_HOST = 'localhost'
SPLUNK_PORT = 8089
SPLUNK_USERNAME = 'shane'
SPLUNK_PASSWORD = 'yrpark12'

# Redis 서버 정보 설정
# REDIS_HOST = '10.0.0.3'
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
redis_db = 0                # Redis 데이터베이스 
splunk_index = 'roka_utm'
sleep_time = 60     # second

# Redis 연결
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=redis_db) 

# Direct Redis 연결
direct_r = DirectRedis(host=REDIS_HOST, port=REDIS_PORT, db=redis_db)

# Splunk 연결
service = client.connect(
    host=SPLUNK_HOST,
    port=SPLUNK_PORT,
    username=SPLUNK_USERNAME,
    password=SPLUNK_PASSWORD
) 

# Splunk 로그 가져오기
def get_splunk_logs():
    current_time = time.time()
    minutes_ago = current_time - (1440 * 60)

    # # Run a Splunk search query
    search_query = f'search index="_internal" earliest={minutes_ago} latest=now()'
    search_args = {'output_mode': 'csv'}

    tm = localtime(minutes_ago)
    print("start search: ", time.strftime("%d-%m-%Y %H:%M:%S", tm))
    search_results = service.jobs.export(search_query, **search_args)

    mid_time = time.time()
    print("start read: %s" % search_query)
    # # Open the search results as a file-like object using io.BytesIO
    logs = io.BytesIO(search_results.read())
    # data = "".join([chunk.decode('utf-8') for chunk in search_results])

    # # 문자열을 Pandas DataFrame으로 변환
    # df = pd.read_csv(StringIO(data))

    # # DataFrame 확인
    # print(df.head())

    fin_time = time.time()
    print(f"search_time: {mid_time-current_time}, read_time: {fin_time-mid_time}")

    return logs

# Redis에 로그 저장
def save_logs_to_redis(logs):
    # Serialize the io.BytesIO object using pickle
    serialized_data = pickle.dumps(logs)
    redis_cluster.set(splunk_index, serialized_data)

    # for log in logs:
    #     b = log.decode('utf-8')  # b' 제거
    #     # frame = json.dumps(b)
    #     # if isinstance(b, dict): # non-empty dict
    #     r.xadd(splunk_index, b, id='*')
    #     # print(f'log: {log}')

    redis_cluster.save()    # Redis 데이터 저장 

# 주기적으로 로그 저장 및 불러오기
def run():
    while True:
        s_time = time.time()
        logs = get_splunk_logs()
        m_time = time.time()
        save_logs_to_redis(logs)
        f_time = time.time()

        read_time = m_time-s_time
        save_time = f_time-m_time
        sum_time = read_time+save_time
        print(f'read_time: {read_time}, save_time: {save_time}, sum_time: {sum_time}')

        results = redis_cluster.get(splunk_index) # 처음부터 끝까지(제거없이 반환)
        # results = r.xread(streams={splunk_index:0}) # 처음부터 끝까지(제거없이 반환)
        # direct_r.delete(splunk_index)  # key 삭제

        start_time = time.time()
        # # read csv data from redis
        # for index, value in enumerate(results):
        #     items = value.decode('utf-8')   # b' 제거
        #     # numpy array로 처리하기 위해서
        #     npa = np.array = items.split(',')
        #     if(index < 2):
        #         print(f"index: {index}, column: {len(npa)}, content: {npa}")    # first 2 contents

        #     # for i in range(len(npa)):
        #     #     if(i+1 == len(npa)):
        #     #         print("{0}".format(npa[i]))
        #     #     else:   print("{0}".format(npa[i]), end=', ')
        # print(f"index: {index}, column: {len(npa)}, content: {npa}")    # last contents

        data = "".join([chunk.decode('utf-8') for chunk in results])

        # 문자열을 Pandas DataFrame으로 변환
        df = pd.read_csv(StringIO(data))

        # DataFrame 확인
        print(df.count)
        # print(df.head())

        end_time = time.time()
        print(f"processing_time : {end_time - start_time}")
        print(f'{sleep_time}초 waiting...')
        time.sleep(sleep_time)     # 1분(60초) 대기 
        print()

# 실행
if __name__ == '__main__':
    run()

    # Close the Splunk connection
    service.logout()