# Splunk 로그를 pandas의 데이터프레임으로 저장하기 위해서는 Splunk REST API를 사용하여 데이터를 조회하고 가져와야 합니다. 
# 아래는 Splunk REST API를 통해 데이터를 가져와서 pandas 데이터프레임으로 저장하는 예제 코드입니다. 

import numpy as np
import requests
import pandas as pd 

# Splunk REST API 엔드포인트와 인증 정보 설정
splunk_url = 'https://10.0.0.3:8089/services/search/jobs/export'
splunk_username = 'admin'
splunk_password = 'demodemo'

# Splunk 쿼리 설정
splunk_query = 'search index="_internal" earliest=-24h latest=now()'
# splunk_query = 'search index="_internal" | head 1000'


# Splunk REST API 요청 파라미터 설정
params = {
    'search': splunk_query,
    'output_mode': 'csv',
    'count': 0
} 

# Splunk REST API에 요청을 보내어 데이터 가져오기
response = requests.get(splunk_url, params=params, auth=(splunk_username, splunk_password), verify=False)
# data = response.json()
data = response.text
lines = data.split('\n')
print(f'line counter: {len(lines)}')

# counter = 0
# for line in lines:
#     print(f"index: {counter}, content: {line}")
#     counter += 1

# 가져온 데이터를 pandas 데이터프레임으로 변환
# df = pd.DataFrame(data['results'], dtype=int)

# 데이터프레임 출력
# print(data)


# 위 코드에서 `'https://your-splunk-instance.com/services/search/jobs/export'`는 Splunk 인스턴스의 URL에 해당하며, 
# `'your-username'`과 `'your-password'`는 Splunk 인스턴스에 사용되는 인증 정보입니다. `'your-index'`는 조회하고자 하는 Splunk 인덱스 이름을 입력하면 됩니다. 
# Splunk REST API를 통해 Splunk 로그 데이터를 가져온 후, `pd.DataFrame()`을 사용하여 데이터를 pandas 데이터프레임으로 변환합니다. 
# 이후 데이터프레임을 원하는 대로 활용할 수 있습니다. 
# 위 코드를 실행하면 Splunk에서 가져온 로그 데이터가 pandas 데이터프레임으로 변환되어 출력됩니다. 가져온 데이터를 필요에 맞게 가공하거나 분석할 수 있습니다.