# Redis 클러스터의 데이터 적재 상태를 확인하기 위해서는 각 Redis 노드에 연결하여 정보를 조회해야 합니다. 
# Redis 클러스터에서는 각 노드가 데이터를 분산 저장하고 있으므로, 모든 노드의 상태를 확인해야 합니다. 
# 아래는 Redis 클러스터의 데이터 적재 상태를 조회하는 Python 코드 예시입니다.

import rediscluster

# Redis 클러스터 노드 설정
REDIS_HOST = 'localhost'
redis_nodes = [
    {"host": REDIS_HOST, "port": 7001},
    {"host": REDIS_HOST, "port": 7002},
    {"host": REDIS_HOST, "port": 7002},
    # 필요한 만큼 Redis 클러스터 노드를 추가하세요.
]

# Redis 클러스터 연결 설정
redis_client = rediscluster.RedisCluster(startup_nodes=redis_nodes)

# Redis 클러스터의 데이터 적재 상태 조회
def get_cluster_data_loading_status():
    cluster_status = {}

    for node in redis_nodes:
        # Redis 노드에 연결
        node_client = rediscluster.RedisCluster(startup_nodes=[node])

        # 데이터 적재 상태 조회
        info = node_client.info("keyspace")

        # 데이터 적재 상태 저장
        cluster_status[node["host"]] = info

        # Redis 노드 연결 종료
        node_client.close()

    return cluster_status

# 데이터 적재 상태 출력
data_loading_status = get_cluster_data_loading_status()
for node, info in data_loading_status.items():
    print(f"Node: {node}")
    print(f"Data Loading Status: {info['db0']['keys']} keys")

# Redis 클라이언트 연결 종료
redis_client.close()

# 위의 코드는 Redis 클러스터의 각 노드에 연결하여 데이터 적재 상태를 조회합니다. 
# `get_cluster_data_loading_status` 함수를 사용하여 모든 노드의 데이터 적재 상태를 조회하고, 결과를 출력합니다. 
# 각 노드의 데이터 적재 상태는 해당 노드의 호스트 이름과 키 개수로 나타내어집니다.
# 이 코드를 실행하면 Redis 클러스터의 각 노드의 데이터 적재 상태를 확인할 수 있습니다.