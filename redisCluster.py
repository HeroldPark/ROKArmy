import requests
import time
from redis import RedisCluster

# Splunk 연결
# Splunk REST API 엔드포인트와 인증 정보 설정
# splunk_url = 'https://10.0.0.3:8089/services/search/jobs/export'
# splunk_username = 'admin'
# splunk_password = 'demodemo'
splunk_url = 'https://localhost:8089/services/search/jobs/export'
splunk_username = 'shane'
splunk_password = 'yrpark12'

redis_host = 'localhost'
# Redis 마스터 클러스터 정보
redis_master_nodes = [
    {'host': redis_host, 'port': '7000'},
    {'host': redis_host, 'port': '7001'},
    {'host': redis_host, 'port': '7002'}
]

# Redis 슬레이브 클러스터 정보
redis_slave_nodes = [
    {'host': redis_host, 'port': '7003'},
    {'host': redis_host, 'port': '7004'},
    {'host': redis_host, 'port': '7005'}
]

# Splunk 로그 가져오기
def get_splunk_logs():
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

    tm = time.localtime(minutes_ago)
    print("start requests: ", time.strftime("%d-%m-%Y %H:%M:%S", tm))

    # Splunk REST API에 요청을 보내어 데이터 가져오기
    response = requests.get(splunk_url, params=params, auth=(splunk_username, splunk_password), verify=False)
    logs = response.text
    lines = logs.split('\n')
    fin_time = time.time()
    print(f"splunk_request_time: {fin_time-current_time}, line_counter: {len(lines)}")

    return logs

# Redis에 데이터 저장
def save_to_redis(data, redis_nodes):
    # RedisCluster configuration options
    redis_config = {
        "startup_nodes": redis_master_nodes,
        "decode_responses": False,  # Set it to True if you want to receive decoded responses
    }
    redis_cluster = RedisCluster(**redis_config)

    # Redis Stream에 데이터 추가
    redis_cluster.xadd('my_stream', data)

    # 연결 닫기
    redis_cluster.close()

# Redis에서 데이터 읽기
def read_from_redis(redis_nodes):
    # Redis 연결 (슬레이브 노드 중 하나 선택)
    redis_config = {
        "startup_nodes": redis_slave_nodes,
        "decode_responses": False,  # Set it to True if you want to receive decoded responses
    }
    redis_cluster = RedisCluster(**redis_config)

    # Redis Stream에서 데이터 읽기
    messages = redis_cluster.xrange('my_stream', '-', '+')

    # 연결 닫기
    redis_cluster.close()

    return messages

# 주기적으로 Splunk 로그 가져오기 및 Redis에 저장하기
while True:
    # Splunk 로그 가져오기
    logs = get_splunk_logs()

    # Redis 마스터 클러스터에 데이터 저장
    save_to_redis(logs, redis_master_nodes)

    # 5분 대기
    time.sleep(60)

    # Redis 슬레이브 클러스터에서 데이터 읽기
    messages = read_from_redis(redis_slave_nodes)

    # 읽은 데이터 처리
    for message in messages:
        print(message)
