# Kafka 기반 비동기 알림 처리로 API 응답 속도 개선
 
> 주문 API에서 무거운 부가 작업(알림 발송)을 동기로 처리했을 때 발생하는 응답 지연 문제를,
> Kafka를 활용한 비동기 이벤트 분리로 해결한 실험 프로젝트입니다.
 
## 1. 프로젝트 배경
 
이전 팀 프로젝트에서 중요한 비즈니스 로직을 처리하는 서버가 "MYSQL에 쓰기작업, 알림 발송, ElasticSearch에 쓰기작업"을
같은 요청 안에서 동기로 처리하면서, API 응답 시간이 길어지는 문제가 발생했습니다.
 
이 프로젝트는 그 문제를 최소 단위로 재현하고, **Kafka로 부가 작업을 비동기 분리했을 때
실제로 얼마나 개선되는지를 정량적으로 검증**하기 위해 설계했습니다.
 
### 가상 시나리오
 
배달 플랫폼에서 고객이 주문을 넣으면, 사장님에게 "새 주문 도착" 알림이 가야 합니다.
이 알림 발송이 주문 API 응답 안에서 동기로 처리되면,
알림 서버가 느려지는 순간 고객의 주문 API 자체도 함께 느려집니다.
반면, 비동기로 처리하면 고객은 알림 발송이 끝나기 전에 API 응답을 받아 속도가 빨라집니다.
 
---
 
## 2. 아키텍처 비교
 
### Before — 동기 처리
 
```
[Client] → POST /orders/sync
              │
              ▼
        주문 저장 (DB)
              │
              ▼
        알림 발송 (1초 소요, 동기 대기)
              │
              ▼
          응답 반환
```
 
### After — Kafka 비동기 분리
 
```
[Client] → POST /orders/async
              │
              ▼
        주문 저장 (DB)
              │
              ▼
    Kafka에 알림 이벤트 발행 (즉시 리턴)
              │
              ▼
          응답 반환  ◀── 여기서 응답 끝
 
   ─────────────────────────────
        (별도 컨슈머가 비동기 처리)
              │
              ▼
        Kafka Consumer가 이벤트 수신
              │
              ▼
        알림 발송 (1초 소요)
              │
              ▼
        DB에 발송 결과 업데이트
```
 
---
 
## 3. 실험 설계
 
| 항목 | 내용 |
|---|---|
| 부하 테스트 도구 | k6 |
| 동시 가상 유저(VUs) | 50명 |
| 테스트 시간 | 30초 |
| 요청 패턴 | 유저당 요청 → 1초 대기(`sleep(1)`) → 재요청 |
| 알림 발송 지연 | `Thread.sleep(1000)`으로 외부 API 호출 지연 시뮬레이션 |
| 측정 지표 | HTTP 응답 시간(avg/median/p90/p95), 처리 건수(RPS) |
 
> 알림 발송은 실제 SMTP/푸시 서버를 연동하지 않고, 인위적 지연(1초)으로 시뮬레이션했습니다.  
> 이 프로젝트의 핵심은 "발송 자체"가 아니라 "동기/비동기 처리 구조의 차이"이기 때문입니다.
 
---
 
## 4. 실험 결과
 
| 지표 | Before (동기) | After (Kafka 비동기) | 개선 |
|---|---|---|---|
| 평균 응답 시간 (avg) | 4.06s | **50.36ms** | 약 **80배** |
| 중앙값 (median) | 4.17s | **11.15ms** | 약 **374배** |
| p90 | 4.29s | 33.99ms | 약 126배 |
| p95 | 4.46s | 158.02ms | 약 28배 |
| 최소 응답 시간 (min) | 1.01s | 2.67ms | - |
| 30초간 처리 건수 | 320건 | **1,450건** | 약 **4.5배** |
| 처리율 (RPS) | 9.26/s | 47.34/s | - |
| 실패율 | 0% | 0% | - |
 
### 원본 k6 결과
 
<details>
<summary>Before (동기) - 전체 로그</summary>
 
```
         /\      Grafana   /‾‾/
    /\  /  \     |\  __   /  /
   /  \/    \    | |/ /  /   ‾‾\
  /          \   |   (  |  (‾)  |
 / __________ \  |_|\_\  \_____/
     execution: local
        script: test.js
        output: -
     scenarios: (100.00%) 1 scenario, 50 max VUs, 1m0s max duration (incl. graceful stop):
              * default: 50 looping VUs for 30s (gracefulStop: 30s)
  █ TOTAL RESULTS
    checks_total.......: 320     9.257382/s
    checks_succeeded...: 100.00% 320 out of 320
    checks_failed......: 0.00%   0 out of 320
    ✓ status is 200
    HTTP
    http_req_duration..............: avg=4.06s min=1.01s med=4.17s max=7.3s  p(90)=4.29s p(95)=4.46s
      { expected_response:true }...: avg=4.06s min=1.01s med=4.17s max=7.3s  p(90)=4.29s p(95)=4.46s
    http_req_failed................: 0.00% 0 out of 320
    http_reqs......................: 320   9.257382/s
    EXECUTION
    iteration_duration.............: avg=5.07s min=2.02s med=5.19s max=8.31s p(90)=5.31s p(95)=5.47s
    iterations.....................: 320   9.257382/s
    vus............................: 10    min=10       max=50
    vus_max........................: 50    min=50       max=50
    NETWORK
    data_received..................: 58 kB 1.7 kB/s
    data_sent......................: 53 kB 1.5 kB/s
running (0m34.6s), 00/50 VUs, 320 complete and 0 interrupted iterations
default ✓ [======================================] 50 VUs  30s
```

</details>

<details>
<summary>After (Kafka 비동기) - 전체 로그</summary>
 
```
         /\      Grafana   /‾‾/
    /\  /  \     |\  __   /  /
   /  \/    \    | |/ /  /   ‾‾\
  /          \   |   (  |  (‾)  |
 / __________ \  |_|\_\  \_____/
     execution: local
        script: test.js
        output: -
     scenarios: (100.00%) 1 scenario, 50 max VUs, 1m0s max duration (incl. graceful stop):
              * default: 50 looping VUs for 30s (gracefulStop: 30s)
  █ TOTAL RESULTS
    checks_total.......: 1450    47.337819/s
    checks_succeeded...: 100.00% 1450 out of 1450
    checks_failed......: 0.00%   0 out of 1450
    ✓ status is 200
    HTTP
    http_req_duration..............: avg=50.36ms min=2.67ms med=11.15ms max=976.76ms p(90)=33.99ms p(95)=158.02ms
      { expected_response:true }...: avg=50.36ms min=2.67ms med=11.15ms max=976.76ms p(90)=33.99ms p(95)=158.02ms
    http_req_failed................: 0.00%  0 out of 1450
    http_reqs......................: 1450   47.337819/s
    EXECUTION
    iteration_duration.............: avg=1.05s   min=1s     med=1.01s   max=1.99s    p(90)=1.04s   p(95)=1.16s
    iterations.....................: 1450   47.337819/s
    vus............................: 50     min=50        max=50
    vus_max........................: 50     min=50        max=50
    NETWORK
    data_received..................: 264 kB 8.6 kB/s
    data_sent......................: 240 kB 7.8 kB/s
running (0m30.6s), 00/50 VUs, 1450 complete and 0 interrupted iterations
default ✓ [======================================] 50 VUs  30s
```

</details>

---
 
## 5. 결과 분석
 
### 5-1. 응답 시간이 극적으로 개선된 이유
 
Before 버전은 API 응답 경로 안에 `Thread.sleep(1000)`(알림 발송)이 그대로 포함되어 있어,
**요청 1건당 최소 1초 이상을 무조건 대기**해야 합니다. 여기에 동시 요청이 몰리면
서버의 스레드/DB 커넥션을 점유한 채 대기하는 요청이 쌓이면서, 평균 응답 시간이
순수 지연시간(1초)의 4배 수준(4.06초)까지 늘어났습니다.
 
After 버전은 알림 발송을 Kafka 이벤트 발행으로 대체했습니다. `kafkaTemplate.send()`는
브로커와 직접 통신하는 게 아니라 **로컬 메모리 버퍼에 메시지를 적재하는 것까지만 수행**하고
즉시 리턴되기 때문에, API는 주문 저장(수 ms) 수준의 시간만으로 응답을 반환할 수 있습니다.
 
### 5-2. avg와 median의 괴리 — 꼬리 지연(Tail Latency)
 
After 버전에서 median(11.15ms)과 avg(50.36ms)의 차이가 큰 점이 눈에 띕니다.
대부분의 요청은 10ms 안팎으로 매우 빠르게 처리되었지만, 일부 요청이 976ms까지 튀면서
평균을 끌어올렸습니다. 이는 주로 테스트 초반 JVM 워밍업, Kafka 프로듀서의 최초 메타데이터
조회, DB 커넥션 풀 초기화 구간에서 발생하는 것으로 추정되며, 정상 구간에서는 median이
보여주듯 매우 안정적인 응답 속도를 유지했습니다.
 
### 5-3. 발견한 트레이드오프 — 처리 위임이지 처리 소멸이 아니다
 
Kafka로 비동기 분리했다고 해서 알림 발송 작업 자체가 사라지는 것은 아닙니다.
API는 빨라졌지만, **실제 알림 처리는 컨슈머 쪽으로 그대로 위임된 것**입니다.
 
컨슈머 설정(`concurrency=5`, 알림 1건당 1초 소요)을 기준으로 계산하면:
 
```
최대 처리 능력 = concurrency(5) × 1건/초 = 초당 5건
테스트 중 유입량 = 1,450건 / 30초 ≈ 초당 48건
 
→ 유입 속도가 처리 속도보다 약 10배 빠르므로, 처리되지 못한 이벤트가
   컨슈머 뒤에 쌓이는 백로그(backlog)가 발생함
   (1,450건을 전부 처리하는 데 이론상 약 4분 50초 소요)
```
---
 
이는 Kafka 도입 시 반드시 함께 고려해야 하는 지점으로, **프로듀서의 유입 속도와
컨슈머의 처리 속도 간 균형**을 맞추지 않으면 "API는 빠른데 실제 알림은 계속 늦게
도착하는" 또 다른 형태의 지연이 발생할 수 있음을 확인했습니다.

## 6. 기술 스택
 
- Spring Boot 4.1.0
- Apache Kafka 4.2.1 (Docker Compose 기반 3-broker KRaft 클러스터)
- MySQL 8.4
- Spring Data JPA / Hibernate
- k6 (부하 테스트)
---
 
## 7. 한계 및 후속 과제
 
- 알림 발송은 실제 외부 연동 없이 `Thread.sleep`으로 시뮬레이션했습니다. 실제 SMTP/FCM
  연동 시 네트워크 실패, 타임아웃 등 추가 변수가 발생할 수 있습니다.
- 컨슈머의 백로그 문제를 발견했지만, concurrency 조정에 따른 실제 end-to-end 지연시간
  개선 효과는 별도로 측정하지 않았습니다. (후속 실험 과제)
- 메시지 처리 실패 시 DLQ(Dead Letter Queue) 등 재처리 전략은 아직 구현하지 않았습니다.
- 로컬 단일 머신 환경에서 진행한 실험으로, 실제 분산 인프라 환경과는 네트워크 지연 등에서 차이가 있을 수 있습니다.
---
 
## 8. 결론
 
무거운 부가 작업을 API 응답 경로에서 Kafka로 비동기 분리함으로써, 동일한 부하 조건
(동시 유저 50명, 초당 약 50건 요청)에서 **평균 응답 시간을 약 80배, 처리량을 약 4.5배
개선**할 수 있음을 실측으로 확인했습니다.
 
다만 이 개선은 "작업이 사라지는 것"이 아니라 "작업이 뒤로 위임되는 것"이라는 점에서,
Kafka 도입 시에는 컨슈머의 처리 능력을 함께 설계해야 한다는 트레이드오프도 함께 확인했습니다.
