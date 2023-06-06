import redis
from rediscluster import RedisCluster

# Redis 클러스터의 노드 정보를 설정합니다.
startup_nodes = [
    {"host": "localhost", "port": "7001"},
    {"host": "localhost", "port": "7002"},
    {"host": "localhost", "port": "7003"},
    {"host": "localhost", "port": "7004"},
    {"host": "localhost", "port": "7005"},
    {"host": "localhost", "port": "7006"},
]

# Redis 클러스터에 연결합니다.
redis_cluster = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)

# 예시로 몇 가지 기본적인 Redis 명령어를 실행해봅니다.
redis_cluster.set("key", "value")
value = redis_cluster.get("key")
print(value)
