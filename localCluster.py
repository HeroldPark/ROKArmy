# redis-5.0.14 에서 테스트 OK
# localhost에서 cluster 구성

from rediscluster import RedisCluster

# Redis 클러스터의 노드 정보 설정
startup_nodes = [
    {"host": "localhost", "port": "7000"},
    {"host": "localhost", "port": "7001"},
    {"host": "localhost", "port": "7002"},
    {"host": "localhost", "port": "7003"},
    {"host": "localhost", "port": "7004"},
    {"host": "localhost", "port": "7006"}
]

# Redis 클러스터 연결
redis_cluster = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)

# 간단한 예시: key-value 저장 및 검색
for i in range(6):
    key = f"key{i}"
    value = f"value{i}"
    
    print(f'port: {redis_cluster}')
    # 데이터 저장
    redis_cluster.set(key, value)
    print(f"Stored: {key} => {value}")
    
    # 데이터 검색
    retrieved_value = redis_cluster.get(key)
    print(f"Retrieved: {key} => {retrieved_value}")
    
# Redis 클러스터 연결 종료
redis_cluster.close()