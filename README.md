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

이 표는 작성 시점 기준이다. 최신 상태는 [이슈](https://github.com/rlatmdgus388/mini-redis/issues)와 PR을 본다.

| 상태 | 내용 |
| --- | --- |
| 동작 | SET / GET / DEL / EXISTS / INCR / EXPIRE / TTL, KEYS(prefix), prefix 삭제, FLUSH, lazy 만료, 타이밍 휠, HTTP API |
| 리뷰 중 | AOF·스냅샷·복구(#12~#14), 200ms 지연 가짜 원본 API(#17) |
| 예정 | Lease 토큰(#10), 네임스페이스 무효화(#11), 샘플 앱과 성능 측정(#18~#21) |

아직 없는 기능(`?lease`, `?ns`, 본문 `lease`)은 조용히 무시하지 않고 501을 돌려준다.

## 알려진 한계

발표에서 먼저 밝히는 항목이다.

1. **O(N) 작업 동안 모든 요청이 멈춘다.** 리사이즈, prefix 삭제, KEYS, 스냅샷 복사는 전역 락을 잡은 채 도는 O(N) 작업이다. 키 상한(100만) 안에서 감수하고, 멈춤 시간은 성능 테스트(#20)로 잰다.
2. **한 번에 명령 하나만 실행한다.** 전역 락, Python GIL, Uvicorn 워커 1개 제약이 모두 같은 방향이라 코어를 여러 개 쓰지 못한다.
3. **서버가 죽으면 최대 약 1초치 쓰기를 잃는다.** AOF를 1초마다 fsync하기 때문이다.
4. **시스템 시계가 바뀌면 만료가 틀어진다.** 영속화를 위해 벽시계(epoch ms)를 택한 대가이다.
5. **무효화된 옛 버전 키는 TTL이 끝날 때까지 메모리를 차지한다.** 네임스페이스 무효화(#11)는 O(1)인 대신 옛 키를 바로 지우지 않는다.

## 성능 측정

아직 측정 전이다. 측정이 끝나면 이 절에 결과 표와 재현 방법을 붙인다.

| 시나리오 | 이슈 | 결과 |
| --- | --- | --- |
| 캐시 있음 vs 없음 (p50·p95, 처리량, hit ratio) | #18 | 측정 전 |
| 스탬피드·Stale set (lease 유무) | #19 | 측정 전 |
| 락 멈춤 (리사이즈, prefix 삭제, 스냅샷 복사) | #20 | 측정 전 |
| 만료 회수 (타이밍 휠 vs lazy만) | #21 | 측정 전 |

원본 API는 200ms 지연을 넣은 가짜 API를 쓴다. 실제 외부 API는 응답 시간이 들쭉날쭉해서 측정에는 쓰지 않고 시연에만 쓴다.

## 스트레치 (시간이 남을 때, 위에서부터)

XFetch 조기 갱신(#23) → 점진적 리해싱(#24) → SIEVE 축출(#25) → prefix 삭제 배치화(#26) → 전역 락 vs 버킷 락 비교(#27)
