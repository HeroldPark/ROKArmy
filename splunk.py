import io
import csv
import time
import redis
import numpy as np
import pickle
import splunklib.client as client

from time import localtime, sleep

# Splunk 서버 정보 설정
SPLUNK_HOST = '10.0.0.3'
SPLUNK_PORT = 8089
SPLUNK_USERNAME = 'admin'
SPLUNK_PASSWORD = 'demodemo' 
# SPLUNK_HOST = 'localhost'
# SPLUNK_PORT = 8089
# SPLUNK_USERNAME = 'shane'
# SPLUNK_PASSWORD = 'yrpark12'

# Redis 서버 정보 설정
# REDIS_HOST = '10.0.0.3'
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
redis_db = 0                # Redis 데이터베이스 
splunk_index = 'roka_utm'

# Redis 연결
# r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=redis_db) 

# Direct Redis 연결
from direct_redis import DirectRedis
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
    search_kwargs = {'output_mode': 'csv'}
    search_results = service.jobs.export(search_query, **search_kwargs)

    # # Open the search results as a file-like object using io.BytesIO
    logs = io.BytesIO(search_results.read())

    # tm = localtime(minutes_ago)
    # print(time.strftime('%Y-%m-%d %H:%M:%S', tm))
    # print("search: %s" % search_query)
    return logs

# Redis에 로그 저장
def save_logs_to_redis(logs):
    # 문자열을 encode하면 byte형이 되고 byte형을 decode하면 문자열이 됩니다.
    # Serialize the io.BytesIO object using pickle
    serialized_data = pickle.dumps(logs)
    direct_r.set(splunk_index, serialized_data)

    # for log in logs:
    #     b = log.decode('utf-8')  # b' 제거
    #     # frame = json.dumps(b)
    #     r.rpush(splunk_index, b)
    #     counter = counter + 1
    #     # print(frame)

    direct_r.save()    # Redis 데이터 저장 

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
        print("read_time: %s" % read_time, ", save_time: %s" % save_time, ", sum_time: %s" % sum_time)

        results = direct_r.get(splunk_index) # 처음부터 끝까지(제거없이 반환)
        # r.lpop(splunk_index)    # 첫번째 요소 제거
        direct_r.delete(splunk_index)  # key 삭제
        # print("results: %s" % results)
        # print(len(results))
        # read csv data from redis 
        # for i in range(len(results)):
        #     if(i == 0):
        #         print(f'first_index: ', i, f', value: ', results[i])
        #     if(i+1 == len(results)):
        #         print(f'last_index: ', i, f', value: ', results[i])
        start_time = time.time()
        # # read csv data from redis 
        count = 0
        for index, value in enumerate(results):
            items = value.decode('utf-8')   # b' 제거
            # list = items.split(',')
            # print("list: %s" % list[0])
            # numpy array로 처리하기 위해서
            npa = np.array = items.split(',')
            # print("numpy.array column counter: {0}".format(len(npa)))
            # for i in range(len(npa)):
            #     if(i+1 == len(npa)):
            #         print("{0}".format(npa[i]))
            #     else:   print("{0}".format(npa[i]), end=', ')
            count += 1
        print(f'length : {count}')
        end_time = time.time()
        print(f"processing_time : {end_time - start_time}")

        time.sleep(60)     # 1분(60초) 대기 
        print('')

# 실행
if __name__ == '__main__':
    run()

    # Close the Splunk connection
    service.logout()