# Mini Redis

해시테이블을 직접 구현한 인메모리 키-값 저장소. Python + FastAPI의 HTTP API로 열고, 느린 API 앞에 캐시(cache-aside)로 붙여 성능 차이를 측정한다.

- 설계 명세: [docs/spec.md](docs/spec.md)
- 협업 규칙: [CONTRIBUTING.md](CONTRIBUTING.md) · 담당자: [docs/assignees.md](docs/assignees.md)
- 발표 개요: [docs/presentation.md](docs/presentation.md)

## TL;DR

`dict` 없이 만든 해시테이블(체이닝, FNV-1a, 0.75에서 2배 확장) 위에, 전역 락 하나로 명령을 한 번에 하나씩 실행하는 Store를 얹었다. 만료는 접근할 때 지우는 lazy 방식과 타이밍 휠로 처리하고, 값 타입은 String 하나뿐이다. 느린 원본 API 앞에서 `GET → miss면 원본 호출 후 TTL과 함께 SET`(cache-aside)으로 쓴다.

## 시작하기

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e '.[dev]'   # 앱 + pytest, httpx
pytest
./scripts/run.sh          # http://localhost:8000
```

> Uvicorn 워커는 **1개**로 고정한다. 워커가 여러 개면 프로세스마다 저장소가 따로 생긴다.

## API 예시

키는 경로에 퍼센트 인코딩해서 넣는다. 아래는 `curl`로 해 본 예시다.

```bash
# 저장 (ttl은 양의 정수 초, 생략하면 무기한)
curl -X PUT localhost:8000/keys/user:1 -H 'content-type: application/json' \
     -d '{"value":"kim","ttl":60}'
# {"ok":true}

# 조회: 남은 ttl(초, 올림)이 함께 온다. 무기한이면 -1
curl localhost:8000/keys/user:1
# {"value":"kim","ttl":60}

# 없거나 만료된 키는 404 (둘을 구분하지 않는다)
curl -i localhost:8000/keys/nope
# 404 {"error":"KEY_NOT_FOUND","message":"..."}

# 존재 확인 / 삭제 (없는 키의 삭제는 에러가 아니라 false)
curl localhost:8000/keys/user:1/exists          # {"exists":true}
curl -X DELETE localhost:8000/keys/user:1       # {"deleted":true}

# 만료 다시 잡기: 남은 시간에 더하지 않고 지금부터 ttl초 (0 이하는 즉시 삭제, 없는 키는 false)
curl -X POST localhost:8000/keys/user:1/expire -H 'content-type: application/json' -d '{"ttl":30}'
# {"updated":true}
curl localhost:8000/keys/user:1/ttl             # {"ttl":30}  (무기한 -1, 없음 -2)

# 카운터: 없는 키는 0에서 시작, 기존 TTL은 유지
curl -X POST localhost:8000/keys/hits/incr -H 'content-type: application/json' -d '{"amount":5}'
# {"value":5}

# 슬래시·공백·한글이 든 키는 퍼센트 인코딩 (a/b 는 a%2Fb)
curl -X PUT 'localhost:8000/keys/a%2Fb' -H 'content-type: application/json' -d '{"value":"x"}'

# 키 묶음
curl 'localhost:8000/keys?prefix=user:'          # {"keys":[...]}
curl -X DELETE 'localhost:8000/keys?prefix=user:' # {"deleted":3}  (빈 prefix는 400)

# 전체 초기화 (confirm 필수)
curl -X POST localhost:8000/admin/flush -H 'content-type: application/json' -d '{"confirm":true}'
```

모든 오류는 `{"error":"CODE","message":"..."}` 형식이다.

| 상태 | 뜻 | 대표 코드 |
| --- | --- | --- |
| 400 | 잘못된 입력 | `INVALID_KEY`, `INVALID_TTL`, `EMPTY_PREFIX`, `CONFIRM_REQUIRED` |
| 404 | 없음·만료 | `KEY_NOT_FOUND` |
| 409 | 상태 충돌 | `NOT_INTEGER` |
| 413 | 값이 1MB 초과 | `VALUE_TOO_LARGE` |
| 507 | 키 개수 상한(100만) | `CAPACITY_EXCEEDED` |
| 501 | 아직 없는 기능 | `NOT_IMPLEMENTED` |

전체 명령 표와 엣지 케이스는 [docs/spec.md](docs/spec.md#외부-api-명세)를 본다.

## 설계 결정 요약

| 항목 | 결정 | 이유 |
| --- | --- | --- |
| 저장소 | 직접 구현한 해시테이블, 체이닝, FNV-1a 64비트, load factor 0.75 초과 시 2배 | 평균 O(1). 내장 `hash()`는 프로세스마다 값이 달라 쓰지 않음 |
| 동시성 | `RLock` 하나로 모든 명령을 직렬 실행 | 단순하고 확실함. Redis의 명령 직렬 실행과 같은 효과 |
| 만료 | lazy(접근 시 삭제) + 계층형 타이밍 휠 | 아무도 읽지 않는 만료 키도 제때 회수 |
| 시간 | 벽시계 epoch ms, 시계는 주입 | 영속화해도 의미가 유지되고 테스트는 가짜 시계로 sleep 없이 가능 |
| 스탬피드·stale set | Lease 토큰 | `SET NX` 락은 낡은 값이 늦게 써지는 것을 못 막음 |
| 영속성 | 스냅샷 + AOF, 1초마다 fsync | 손실은 최대 약 1초, AOF가 무한히 커지지 않음 |
| 값 타입 | String만 (INCR은 정수로 해석) | 과제 핵심은 해시테이블·TTL·동시성 |

## 구조

```
mini_redis/
├── hashtable.py      직접 구현한 해시테이블 (dict/set/collections 금지)
├── store.py          전역 RLock, 모든 명령의 진입점
├── timing_wheel.py   계층형 타이밍 휠 + 만료 스레드
├── api/              FastAPI 앱과 라우트 (검증만, 상태는 Store를 통해서만)
├── persistence/      AOF, 스냅샷, 복구
├── lease.py, namespace.py
tests/unit, tests/functional    bench/    sample_app/
```

호출 방향은 **API → Store → HashTable** 한 방향뿐이다.

## 구현 현황

이슈 #1~#27은 모두 구현됐다. 남은 것은 이 문서의 성능 결과 정리(#22)뿐이었다.

| 영역 | 내용 |
| --- | --- |
| 명령 | SET / GET / DEL / EXISTS / INCR / EXPIRE / TTL, KEYS(prefix), prefix 삭제, FLUSH |
| 만료 | lazy 만료, 계층형 타이밍 휠 |
| 캐시 보호 | Lease 토큰(스탬피드·stale set), 네임스페이스 버전 무효화, XFetch 조기 갱신 |
| 영속성 | AOF(1초 fsync), 스냅샷, 기동 시 복구 |
| 스트레치 | 점진적 리해싱(기본 동작), SIEVE 축출(`eviction="sieve"`), prefix 삭제 배치(`?batch=`), 락 쪼개기 처리량 비교(측정만) |

## 알려진 한계

발표에서 먼저 밝히는 항목이다.

1. **O(N) 작업 동안 모든 요청이 멈춘다.** 리사이즈, prefix 삭제, KEYS, 스냅샷 복사는 전역 락을 잡은 채 도는 O(N) 작업이다. 키 상한(100만) 안에서 감수한다. 키 100만 개에서 잰 최대 대기는 아래 [성능 측정](#성능-측정)에 있다(prefix 삭제 약 340ms, 스냅샷 복사 약 350ms).
2. **한 번에 명령 하나만 실행한다.** 전역 락, Python GIL, Uvicorn 워커 1개 제약이 모두 같은 방향이라 코어를 여러 개 쓰지 못한다.
3. **서버가 죽으면 최대 약 1초치 쓰기를 잃는다.** AOF를 1초마다 fsync하기 때문이다.
4. **시스템 시계가 바뀌면 만료가 틀어진다.** 영속화를 위해 벽시계(epoch ms)를 택한 대가이다.
5. **무효화된 옛 버전 키는 TTL이 끝날 때까지 메모리를 차지한다.** 네임스페이스 무효화는 O(1)인 대신 옛 키를 바로 지우지 않는다.

## 성능 측정

원본은 200ms 지연을 넣은 가짜 API다. 실제 외부 API는 응답 시간이 들쭉날쭉해서 측정에는 쓰지 않고 시연에만 쓴다.
아래 숫자는 `bench/results/`의 결과 파일에서 가져왔고, 재현 방법은 [bench/README.md](bench/README.md)에 있다.
벽시계를 쓰는 측정은 Windows 11, CPU 논리 코어 14개, Python 3.14.7에서 돌렸다. 다른 컴퓨터에서는 절대값이 달라지고 방향만 같다.

| 시나리오 | 비교 | 결과 | 자세히 |
| --- | --- | --- | --- |
| 캐시 효과 (#18) | 샘플 앱 캐시 끔 vs 켬 | 이후 요청 p50 약 219ms → 26~34ms, p95 약 230ms → 54~72ms, 처리량 약 73 → 414~539 req/s, 원본 호출 2,000 → 0 | [cache_effect.md](bench/results/cache_effect.md) |
| 스탬피드 (#19) | 만료 순간 동시 100요청, lease 없음 vs 있음 | 원본 호출 100 → 1 | [lease_scenarios.md](bench/results/lease_scenarios.md) |
| Stale set (#19) | DEL과 늦은 PUT 경합 | 겹치는 구간에서 낡은 값이 남는 비율 100% → 0% (전체 50.1% → 0%) | 같은 파일 |
| 조기 갱신 (#23) | 초당 100건 인기 키, lease만 vs lease + XFetch | 값을 기다린 요청 904개(7.66%) → 0개, 원본 호출은 11 vs 12로 거의 같음 | [xfetch.md](bench/results/xfetch.md) |
| 락 멈춤 (#20) | 키 100만 개에서 O(N) 작업 중 다른 get의 최대 대기(중앙값) | prefix 삭제 343ms, KEYS 219ms, 스냅샷 복사 351ms | [lock_pause.md](bench/results/lock_pause.md) |
| 리사이즈 (#24) | 일괄 vs 점진적 리해싱 (put 한 번의 최대 지연) | 953ms → 1.7ms (대신 p99 2.4µs → 10.7µs) | [rehash_latency.md](bench/results/rehash_latency.md) |
| 만료 회수 (#21) | 키 10만 개, 타이밍 휠 vs lazy만 | TTL이 퍼져 있으면 70초 뒤 남은 만료 키 90,606 → 0 | [expiry_reclaim.md](bench/results/expiry_reclaim.md) |
| 락 쪼개기 (#27) | 전역 락 vs 버킷 락 처리량 (읽기 100%, 스레드 8, 락 256개) | GIL 켜짐 0.70배(오히려 느림), GIL 꺼짐 3.33배 | [GIL](bench/results/lock_striping_gil.md) · [free-threaded](bench/results/lock_striping_nogil.md) |

### 읽을 때 주의할 점

- **캐시 효과**: 첫 요청은 둘 다 원본까지 가서 p50이 비슷하다(약 225~242ms). 다만 p95는 캐시를 켠 쪽이 약 40~55ms 더 느렸다(282~303ms vs 243~250ms). miss 뒤 PUT 한 번이 더 붙기 때문으로 짐작하지만 원인까지 재지는 않았다. 이득은 같은 키를 다시 읽을 때 나온다.
- **만료 회수**: 10초 안에 전부 만료되는 burst에서는 휠도 tick당 1,000개 상한 때문에 다 지우기까지 약 10초가 걸린다. 그래도 lazy만 쓰면 70초 뒤에도 90,028개가 남는다.
- **스탬피드**: 원본 호출은 100 → 1로 줄지만 Mini Redis로 가는 요청은 200 → 299로 늘어난다. 대기자가 100ms마다 다시 묻기 때문이다. 가장 늦은 응답은 둘 다 200ms다.
- **조기 갱신**: 줄어드는 것은 원본 호출 수가 아니라 호출을 기다리는 사용자다. XFetch는 확률적이라 요청이 드물면(초당 20건 이하) 놓치는 주기가 있다.
- **락 멈춤**: 점진적 리해싱과 나눠서 지우기(`?batch=`)는 최대 대기를 약 30ms로 줄이는 대신 작업 전체 시간이 길어진다(prefix 삭제 약 300~380ms → 약 560~680ms). 나눠서 지우기는 원자적이지 않다. 락 없는 파이썬 작업만 있어도 약 16ms가 생기는데 GIL 때문이다.
- **리사이즈**: 평균 비용은 조금 오른다(100만 개 넣는 합계 3.47초 → 3.55초). 점진적 리해싱은 꼬리를 줄이고 평균을 내주는 선택이다.
- **락 쪼개기**: GIL이 켜진 보통 파이썬에서는 일관된 이득이 없었다(읽기 100%는 0.70~0.80배, 읽기 90%는 0.94~1.34배로 들쭉날쭉). 그래서 우리 서버는 전역 락을 그대로 쓴다. GIL이 없는 파이썬(free-threaded)에서는 락을 256개로 쪼개면 읽기 100%·스레드 8개에서 3.33배였지만 전역 락은 스레드가 늘수록 0.18배까지 떨어졌다. 리사이즈와 삭제가 없는 단순한 표로 잰 값이라 실제 이득은 이보다 작다.

## 스트레치

모두 구현했다: XFetch 조기 갱신(#23), 점진적 리해싱(#24), SIEVE 축출(#25), prefix 삭제 배치화(#26), 전역 락 vs 버킷 락 비교(#27).
