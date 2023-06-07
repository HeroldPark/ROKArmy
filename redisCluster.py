# 아래는 3개의 Redis 클러스터를 만들고, Splunk 로그를 5분마다 가져와 Redis 클러스터에 순차적으로 중복되지 않게 돌아가면서 스트리밍하는 Python 코드입니다. 
# 동시에 데이터를 쓰는 Redis 클러스터 중에서 작업이 완료된 클러스터에서만 데이터를 읽어오고, 사용이 완료된 클러스터는 삭제합니다. 
# 코드를 통해 Redis 클러스터에 연결하고 스트림에 데이터를 쓰고 읽는 방법을 보여줍니다. 필요에 따라 코드를 수정하여 사용하실 수 있습니다.

from sched import scheduler
import time
from rediscluster import RedisCluster
import requests
import urllib3

# Splunk REST API 엔드포인트 및 인증 정보 설정
splunk_endpoint = "https://10.0.0.3:8089/rest/search/jobs"
splunk_username = "admin"
splunk_password = "demodemo"

REDIS_HOST = 'localhost'
startup_nodes = [
    {"host": REDIS_HOST, "port": "7001"},
    {"host": REDIS_HOST, "port": "7002"},
    {"host": REDIS_HOST, "port": "7003"}
    # 필요한 만큼 Redis 클러스터 노드를 추가하세요.
]

sleep_time = 60     # second

# Redis 클러스터 연결 설정
redis_config = {
    "startup_nodes": startup_nodes,
    "decode_responses": False,  # Set it to True if you want to receive decoded responses
    "skip_full_coverage_check": True
}
# Create a Redis cluster object
redis_cluster = RedisCluster(**redis_config)

redis_index = 0
# stream_key = "roka_utm"+redis_index
stream_key = "roka_utm"
# Splunk 로그 가져오기 및 Redis 스트림에 저장
def fetch_splunk_logs_and_store_in_redis():
    global redis_index

    # redis_client = redis_cluster[redis_index]

    current_time = time.time()
    minutes_ago = current_time - (1 * 60)

    # # Run a Splunk search query
    splunk_query = f'search index="_internal" earliest={minutes_ago} latest=now()'

    # Splunk REST API 요청 파라미터 설정
    params = {
        'search': splunk_query,
        'output_mode': 'csv',
        'count': 0
    } 

    tm = time.localtime(minutes_ago)
    print("start requests: ", time.strftime("%d-%m-%Y %H:%M:%S", tm))

    # Splunk REST API에 요청을 보내어 데이터 가져오기
    response = requests.get(splunk_endpoint, params=params, auth=(splunk_username, splunk_password), verify=False)

    if response.status_code == 200:
        splunk_logs = response.text
        for log in splunk_logs["results"]:
            # 필요한 데이터 필드 추출 (예: log['timestamp'], log['message'])
            # 필드를 필요에 맞게 수정하여 사용하십시오.
            log_data = {
                "timestamp": log["timestamp"],
                "message": log["message"]
            }
            print(f"write stream_key: {stream_key}")
            # Redis 스트림에 데이터 쓰기
            redis_client.xadd(stream_key, fields=log_data)

        # Redis 클러스터 인덱스 업데이트
        redis_index = (redis_index + 1) % 3
        stream_key = "roka_utm"+redis_index

def read_from_redis_stream(redis_client):
    # 스트림 ID 조회
    print(f"read stream_key: {stream_key}")
    last_id = redis_client.xinfo_stream(stream_key)["last-generated-id"]

    # 스트림 데이터 읽기
    stream_data = redis_client.xread({stream_key: last_id}, block=0)

    for _, messages in stream_data:
        for message_id, message in messages:
            # 필요한 데이터 처리 (예: message['timestamp'], message['message'])
            # 필요한 데이터 처리 로직을 추가하세요.
            print(f"Message ID: {message_id}")
            print(f"Message: {message}")

# 프로그램 실행
if __name__ == "__main__":
    urllib3.disable_warnings()
    while True:
        # 주기적으로 Splunk 로그 가져오기 및 Redis 스트림에 저장
        # fetch_and_store_logs()
        fetch_splunk_logs_and_store_in_redis()

        # 작업이 완료된 Redis 클러스터에서만 데이터 읽기
        for redis_client in redis_cluster:
            read_from_redis_stream(redis_client)

        # Redis 클러스터 삭제
        for redis_client in redis_cluster:
            redis_client.flushall()

        print(f'{sleep_time}초 waiting...')
        time.sleep(sleep_time)     # 1분(60초) 대기 
        print()
