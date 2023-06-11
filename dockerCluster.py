import redis
from rediscluster import RedisCluster

# Redis 클러스터 노드의 호스트 및 포트 정보 설정
redis_nodes = [
    {'host': 'localhost', 'port': 7000},
    {'host': 'localhost', 'port': 7001},
    {'host': 'localhost', 'port': 7002},
    {'host': 'localhost', 'port': 7003},
    {'host': 'localhost', 'port': 7004},
    {'host': 'localhost', 'port': 7005}
]

# Redis 클러스터 연결 설정
# redis_cluster = redis.RedisCluster(startup_nodes=redis_nodes, decode_responses=True)
redis_cluster = RedisCluster(startup_nodes=redis_nodes, decode_responses=True)

# Redis 명령 실행 예시
redis_cluster.set('mykey', 'myvalue')
value = redis_cluster.get('mykey')
print(value)