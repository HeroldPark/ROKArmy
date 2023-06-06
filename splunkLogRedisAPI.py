import io
import csv
import json
import time
import redis
import numpy as np
import pickle
import requests
import splunklib.client as client
import splunklib.results as results
from time import localtime, sleep
from direct_redis import DirectRedis

import urllib3

# Splunk 서버 정보 설정
SPLUNK_HOST = '10.0.0.3'
SPLUNK_PORT = 8089
SPLUNK_USERNAME = 'admin'
SPLUNK_PASSWORD = 'demodemo'

# Redis 서버 정보 설정
# REDIS_HOST = '10.0.0.3'
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
redis_db = 0                # Redis 데이터베이스 
splunk_index = 'roka_utm'
sleep_time = 60     # second

# Redis 연결
rds = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=redis_db) 

# Direct Redis 연결
direct_r = DirectRedis(host=REDIS_HOST, port=REDIS_PORT, db=redis_db)

# xadd, xdel, xrange 사용할때
conn = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=redis_db)

# Splunk REST API 엔드포인트와 인증 정보 설정
splunk_url = 'https://10.0.0.3:8089/services/search/jobs/export'
splunk_username = 'admin'
splunk_password = 'demodemo'

# Splunk 로그 REST API 방식으로 가져오기
def get_splunk_logs_by_restAPI():
    current_time = time.time()
    minutes_ago = current_time - (1440 * 60)

    # # Run a Splunk search query
    # splunk_query = f'search index="_internal" earliest={minutes_ago} latest=now()'
    splunk_query = 'search index="_internal" | head 100'

    # Splunk REST API 요청 파라미터 설정
    params = {
        'search': splunk_query,
        'output_mode': 'csv',
        'count': 0
    }

    tm = localtime(minutes_ago)
    print("start requests: ", time.strftime("%d-%m-%Y %H:%M:%S", tm))

    # Splunk REST API에 요청을 보내어 데이터 가져오기
    response = requests.get(splunk_url, params=params, auth=(splunk_username, splunk_password), verify=False)
    logs = response.text
    lines = logs.split('\n')
    fin_time = time.time()
    print(f"splunk_request_time: {fin_time-current_time}, line_counter: {len(lines)}")

    return logs

# Redis에 로그 저장
def save_logs_to_redis(logs):
    # 1. 스트림 데이터 쓰기(set, get)
    # splunk log를 csv으로 가져와야 함
    rds.set(splunk_index, logs)
    lines = logs.split('\n')
    print(f'first line: {lines[0]}')

    # 2. 스트림 데이터 쓰기(rpush, lrange)
    # splunk log를 csv으로 가져와야 함
    # lines = logs.split('\n')
    # for line in lines:
    #     # print(f'line: {line}')
    #     rds.rpush(splunk_index, line)

    # 3. 스트림 데이터 쓰기(xadd, xread)
    # splunk log를 json으로 가져와야 함
    # splunk_logs = logs.split('\n')
    # # logs.pop(0)
    # for log in splunk_logs:
    #     rds.xadd(splunk_index, {'log': log} )

    # rds.save()    # Redis 데이터 저장 

# 주기적으로 로그 저장 및 불러오기
def run():
    urllib3.disable_warnings()
    while True:
        s_time = time.time()
        logs = get_splunk_logs_by_restAPI()
        
        m_time = time.time()
        save_logs_to_redis(logs)
        f_time = time.time()

        read_time = m_time-s_time
        save_time = f_time-m_time
        sum_time = read_time+save_time
        print(f'splunk_request_time: {read_time}, redis_save_time: {save_time}, sum_time: {sum_time}')

        start_time = time.time()

        # 1. 스트림 데이터 읽기(set, get)
        results = rds.get(splunk_index)
        lines = results.decode('utf-8')
        line_count = lines.count('\n') + 1
        # for line in results:
        #   print(line.decode('utf-8'))

        # 2. 스트림 데이터 읽기(rpush, lrange)
        # results = rds.lrange(splunk_index, 0, -1)   # 처음부터 끝까지 읽는다.
        # lines = [item.decode('utf-8') for item in results]
        # line_count = len(lines)

        # 3. 스트림 데이터 읽기(xadd, xread)
        # rds.xadd()은 정상인데 rds.xread()는 시간이 지나치게 오래 소요됨.(문제가 있어 보임)
        # last_id = rds.xinfo_stream(splunk_index)["last-generated-id"]
        # results = rds.xread({splunk_index: last_id}, block=0)
        # lines = results.decode('utf-8')
        # line_count = len(lines)
        # # for line in results:
        # #   print(line.decode('utf-8'))

        end_time = time.time()

        # 4. 스트림 데이터 삭제
        rds.delete(splunk_index)    # ram 상의 데이터 삭제

        print(f"redis_read_time : {end_time - start_time}, line_count: {line_count}")
        print(f'{sleep_time}초 waiting...')
        time.sleep(sleep_time)     # 1분(60초) 대기 
        print()

# 실행
if __name__ == '__main__':
    run()