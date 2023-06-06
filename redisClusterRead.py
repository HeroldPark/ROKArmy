import time
from rediscluster import RedisCluster 

REDIS_HOST = "localhost"
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

splunk_index = 'roka_utm'
sleep_time = 1     # second

# Create a Redis cluster object
redis_cluster = RedisCluster(**redis_config)

# 주기적으로 로그 저장 및 불러오기
def read_or_wait():
    while True:
        start_time = time.time()
        # 풀에 데이터가 있는지 확인
        # results = redis_cluster.lpop(splunk_index) # 처음부터 끝까지(제거없이 반환)
        # results = redis_cluster.get(splunk_index) # 처음부터 끝까지(제거없이 반환)
        # data = results.decode('utf-8')
        # lines = data.split('\n')

        results = redis_cluster.lrange(splunk_index, 0, -1)   # 처음부터 끝까지 읽는다.
        redis_cluster.delete(splunk_index)    # ram 상의 데이터 삭제
        lines = [item.decode('utf-8') for item in results]
        line_count = len(lines)

        end_time = time.time()
        print(f"processing_time : {end_time - start_time}, line_count: {line_count}")
        print(f'{sleep_time}초 waiting...')
        time.sleep(sleep_time)     # 1분(60초) 대기 
        print()

# 실행
if __name__ == '__main__':
    read_or_wait()