import time
import redis
import splunklib.client as splunk

SPLUNK_HOST = 'localhost'
SPLUNK_PORT = 8089
SPLUNK_USERNAME = 'shane'
SPLUNK_PASSWORD = 'yrpark12'

# Redis 클러스터에 연결하는 함수
def connect_to_redis_cluster():
    redis_host = 'localhost'
    redis_nodes = [
        {'host': redis_host, 'port': 7001},
        {'host': redis_host, 'port': 7002},
        {'host': redis_host, 'port': 7003}
        # {'host': redis_host, 'port': 7004},
        # {'host': redis_host, 'port': 7005},
        # {'host': redis_host, 'port': 7006}
    ]
    return redis.RedisCluster(startup_nodes=redis_nodes, decode_responses=True)

# Splunk 인덱스에서 데이터를 가져오는 함수
def get_data_from_splunk_index():
    current_time = time.time()
    minutes_ago = current_time - (1440 * 60)

    service = splunk.connect(host=SPLUNK_HOST, port=SPLUNK_PORT, username=SPLUNK_USERNAME, password=SPLUNK_PASSWORD)
    # index_name = '_internal'
    search_query = f'search index="_internal" earliest={minutes_ago} latest=now()'
    search_args = {'output_mode': 'csv'}
    
    # index = service.indexes[index_name]
    # search_results = index.search(search_query)
    
    # data = []
    # for result in search_results:
    #     data.append(result['your_field'])

    data = service.jobs.export(search_query, **search_args)
    
    return data

# Redis 클러스터에 데이터를 쓰는 함수
def write_data_to_redis_cluster(redis_cluster, data):
    for i, value in enumerate(data):
        redis_cluster.set(f'key_{i}', value)

# Redis 클러스터에서 데이터를 읽는 함수
def read_data_from_redis_cluster(redis_cluster):
    data = []
    for i in range(len(redis_cluster.connection_pool.nodes)):
        value = redis_cluster.get(f'key_{i}')
        if value is not None:
            data.append(value)
    
    return data

# Redis 클러스터에 연결
redis_cluster = connect_to_redis_cluster()

# Splunk 인덱스에서 데이터 가져오기
data_from_splunk = get_data_from_splunk_index()

# Redis 클러스터에 데이터 쓰기
write_data_to_redis_cluster(redis_cluster, data_from_splunk)

# Redis 클러스터에서 데이터 읽기
data_from_redis = read_data_from_redis_cluster(redis_cluster)

# 결과 출력
print("Data from Splunk:", data_from_splunk)
print("Data from Redis:", data_from_redis)