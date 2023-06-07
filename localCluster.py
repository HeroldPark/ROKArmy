# 아래에는 Redis 클러스터를 사용하여 localhost의 7001부터 6개의 포트를 설정하고 순차적으로 불러와서 사용하는 Python 코드의 예시가 있습니다. 
# 코드를 실행하기 전에 redis-py 라이브러리를 설치해야 합니다.

from rediscluster import RedisCluster

# Redis 클러스터의 노드 정보 설정
startup_nodes = [
    {"host": "localhost", "port": "7001"},
    {"host": "localhost", "port": "7002"},
    {"host": "localhost", "port": "7003"},
    {"host": "localhost", "port": "7004"},
    {"host": "localhost", "port": "7005"},
    {"host": "localhost", "port": "7006"}
]

# Redis 클러스터 연결
redis_cluster = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)

# 간단한 예시: key-value 저장 및 검색
for i in range(6):
    key = f"key{i}"
    value = f"value{i}"
    
    # 데이터 저장
    redis_cluster.set(key, value)
    print(f"Stored: {key} => {value}")
    
    # 데이터 검색
    retrieved_value = redis_cluster.get(key)
    print(f"Retrieved: {key} => {retrieved_value}")
    
# Redis 클러스터 연결 종료
redis_cluster.close()

# 이 코드에서는 rediscluster 라이브러리를 사용하여 Redis 클러스터에 연결합니다. 
# `startup_nodes` 변수에 Redis 클러스터의 각 노드의 호스트와 포트 정보를 지정합니다. 
# 그런 다음 `RedisCluster` 클래스의 인스턴스를 만들어 Redis 클러스터에 연결합니다.
# `for` 루프를 사용하여 6개의 키와 값을 Redis 클러스터에 저장하고 검색하는 예시를 제공합니다. 
# `redis_cluster.set(key, value)`를 사용하여 데이터를 저장하고, `redis_cluster.get(key)`를 사용하여 저장된 값을 검색합니다.
# 마지막으로 `redis_cluster.close()`를 호출하여 Redis 클러스터와의 연결을 종료합니다.
# 이 코드는 Redis 클러스터의 각 노드에 대해 순차적으로 작업을 수행합니다. 따라서 7001, 7002, 7003, 7004, 7005, 7006 순서대로 작업이 진행됩니다.