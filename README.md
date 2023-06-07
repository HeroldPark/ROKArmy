> 1. splunklog.py 오류
    Exception has occurred: AuthenticationError
    Request failed: Session is not logged in.
    splunklib.binding.HTTPError: HTTP 401 Unauthorized -- call not properly authenticated

> 2. 오류  
    local variable 'counter' referenced before assignment
    => global count

> 3. localhost에서 search 기간을 30일로 했을때 발생.
    Exception has occurred: ConnectionError
    Error 10053 while writing to socket. 현재 연결은 사용자의 호스트 시스템의 소프트웨어의 의해 중단되었습니다.
    ConnectionAbortedError: [WinError 10053] 현재 연결은 사용자의 호스트 시스템의 소프트웨어의 의해 중단되었습니다

> 4. redis cluster 환경에서 시험할때 발생...
    Exception has occurred: SlotNotCoveredError
    Slot "13907" not covered by the cluster. "skip_full_coverage_check=True"  
    KeyError: 13907
    During handling of the above exception, another exception occurred:  
    File "C:\Users\DeltaX_20\Documents\Workspace\ROKArmy\redisCluster.py", line 104, in <module>
        for redis_client in redis_cluster:
    rediscluster.exceptions.SlotNotCoveredError: Slot "13907" not covered by the cluster.  "skip_full_coverage_check=True"

> 5. 