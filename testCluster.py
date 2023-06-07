import redis
from rediscluster import RedisCluster
from multiprocessing import Pool

# Redis 클러스터의 노드 목록
startup_nodes = [
        {"host": "localhost", "port": "7001"},
        {"host": "localhost", "port": "7002"},
        {"host": "localhost", "port": "7003"},
        {"host": "localhost", "port": "7004"},
        {"host": "localhost", "port": "7005"},
        {"host": "localhost", "port": "7006"}
    ]


# Redis 클라이언트를 생성합니다.
# Redis 클러스터에 연결하려면 'redis.ClusterConnectionPool'을 사용합니다.
# 노드 리스트에는 클러스터의 모든 노드 주소와 포트를 지정합니다.

r = RedisCluster(startup_nodes=startup_nodes, decode_responses=True, skip_full_coverage_check=True)

# Redis 클러스터에 명령을 전송합니다.
r.set('key', 'value')
value = r.get('key')
print(value)
