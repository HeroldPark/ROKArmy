# Redis 클러스터에서 6개의 인스턴스 중 4개가 쓰기 기능을 처리하고 2개가 읽기 기능을 병렬로 처리하는 파이썬 코드입니다. 
# 이 코드는 `redis-py-cluster` 라이브러리를 사용하여 Redis 클러스터와 상호 작용합니다.
# ```python

from rediscluster import RedisCluster
from multiprocessing import Pool

REDIS_HOST = 'localhost'
REDIS_KEY = 'roka_utm'

def write_to_redis(value):
    startup_nodes = [
        {"host": REDIS_HOST, "port": 7001},
        {"host": REDIS_HOST, "port": 7002},
        {"host": REDIS_HOST, "port": 7003},
        {"host": REDIS_HOST, "port": 7004}
    ]
    r = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    r.set(REDIS_KEY, value)

def read_from_redis():
    startup_nodes = [
        {"host": REDIS_HOST, "port": 7005},
        {"host": REDIS_HOST, "port": 7006}
    ]
    r = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    return r.get(REDIS_KEY)

if __name__ == '__main__':
    pool = Pool(processes=6)  # 총 6개의 프로세스를 사용합니다.
    write_values = ['value1', 'value2', 'value3', 'value4']
    pool.map(write_to_redis, write_values)  # 4개의 인스턴스에 병렬로 쓰기 작업을 수행합니다.

    read_results = pool.map(read_from_redis, range(2))  # 2개의 인스턴스에서 병렬로 읽기 작업을 수행합니다.
    for result in read_results:
        print(result)
# ```

# 위의 코드에서 'your_host1', 'your_port1' 등을 Redis 클러스터의 실제 호스트와 포트로 대체해야 합니다. 
# 코드에서 `write_to_redis` 함수는 Redis 클러스터에 값을 저장하고 `read_from_redis` 함수는 Redis 클러스터에서 값을 읽어옵니다. 
# `Pool` 객체를 사용하여 병렬로 작업을 처리하고 `map` 메서드를 사용하여 작업을 프로세스 풀에 매핑합니다.

# 위의 코드는 4개의 인스턴스에서 동시에 쓰기 작업을 수행하고 2개의 인스턴스에서 병렬로 읽기 작업을 수행합니다. 
# 읽기 작업은 `range(2)`를 사용하여 2번 호출되며, 각 호출에서 `read_from_redis` 함수가 병렬로 실행되어 Redis 클러스터에서 값을 읽어옵니다.

# 실행 결과는 읽어온 값들이 출력됩니다.