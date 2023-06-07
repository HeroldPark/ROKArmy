import time
import requests
from time import localtime
from rediscluster import RedisCluster
import urllib3 

REDIS_HOST = "localhost"
REDIS_PORT = 6379
redis_db = 0                # Redis 데이터베이스 
sleep_time = 60     # second

# Define the Redis cluster nodes and configuration
startup_nodes = [
    {"host": REDIS_HOST, "port": "7001"},
    {"host": REDIS_HOST, "port": "7002"},
    {"host": REDIS_HOST, "port": "7003"}
    # {"host": REDIS_HOST, "port": "7004"},
    # {"host": REDIS_HOST, "port": "7005"},
    # {"host": REDIS_HOST, "port": "7006"}
    # Add more nodes if necessary
]

# RedisCluster configuration options
redis_config = {
    "startup_nodes": startup_nodes,
    "decode_responses": False,  # Set it to True if you want to receive decoded responses
}

# Create a Redis cluster object
redis_cluster = RedisCluster(**redis_config)

# Splunk REST API 엔드포인트와 인증 정보 설정
splunk_url = 'https://10.0.0.3:8089/services/search/jobs/export'
splunk_username = 'admin'
splunk_password = 'demodemo'
splunk_index = 'roka_utm'

# Splunk 로그 REST API 방식으로 가져오기
def get_splunk_logs_by_restAPI():
    current_time = time.time()
    minutes_ago = current_time - (1440 * 60)

    # # Run a Splunk search query
    splunk_query = f'search index="_internal" earliest={minutes_ago} latest=now()'

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
    print(f"requests_time: {fin_time-current_time}, line_counter: {len(lines)}")

    return logs

# Redis에 로그 저장
def save_logs_to_redis(logs):
    counter = 0
    redis_st = time.time()
    # redis_cluster.set(splunk_index)
    lines = logs.split('\n')
    for line in lines:
        redis_cluster.rpush(splunk_index, line)
    # for log in logs:
    #     log = log.decode('utf-8')
    #     redis_cluster.rpush(splunk_index, log)
    #     counter += 1

    # redis_cluster.lpush(splunk_index, data)
    redis_md = time.time()
    print(f'redis_set_time : {redis_md - redis_st}')
    redis_cluster.save()    # Redis 데이터 저장 
    redis_et = time.time()
    print(f'redis_save_time : {redis_et - redis_md}')
    print(f'logs counter : {counter}')

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
        print(f'read_time: {read_time}, save_time: {save_time}, sum_time: {sum_time}')
        print(f'{sleep_time}초 waiting...')
        time.sleep(sleep_time)     # 1분(60초) 대기 
        print()

# 실행
if __name__ == '__main__':
    run()