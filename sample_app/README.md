# 샘플 앱

## 가짜 원본 API (200ms 지연)

cache-aside 성능 비교에서 "느린 원본" 역할을 하는 가짜 API예요.
호출 횟수를 세어 두니까, 캐시가 원본 호출을 얼마나 줄였는지 확인할 수 있어요.

실행해요.

    uvicorn sample_app.origin:app --port 8001

| 엔드포인트 | 설명 |
| --- | --- |
| `GET /items/{item_id}` | 200ms 기다린 뒤 `{"id": ..., "value": "item-<id>"}`를 돌려줘요. 호출 수가 1 올라가요. |
| `GET /stats` | 지금까지의 호출 수 `{"calls": n}`를 바로 돌려줘요. 호출로 세지 않아요. |
| `POST /stats/reset` | 호출 수를 0으로 되돌리고 `{"calls": 0}`를 돌려줘요. |

## cache-aside 샘플 앱

Mini Redis를 앞에 두고 가짜 원본을 감싸는 앱이에요. 캐시에 있으면 바로 돌려주고(HIT), 없으면 원본을 불러 60초 동안 캐시에 채워요(MISS).

프로세스 세 개를 띄워요.

    # 1) Mini Redis (:8000) - scripts/run.sh 또는 uvicorn
    # 2) 가짜 원본 (:8001)
    uvicorn sample_app.origin:app --port 8001
    # 3) 샘플 앱 (:8002)
    uvicorn sample_app.app:app --port 8002

환경 변수로 접속 주소를 바꿀 수 있어요.

| 변수 | 기본값 | 설명 |
| --- | --- | --- |
| `MINI_REDIS_URL` | `http://127.0.0.1:8000` | Mini Redis 주소 |
| `ORIGIN_URL` | `http://127.0.0.1:8001` | 가짜 원본 주소 |
| `SAMPLE_USE_CACHE` | (켜짐) | `0`이면 캐시를 끄고 매번 원본을 불러요 |

| 엔드포인트 | 설명 |
| --- | --- |
| `GET /items/{item_id}` | 응답 헤더 `X-Cache`가 `HIT`(캐시에서)나 `MISS`(원본에서)예요. 캐시가 고장 나면 `502 CACHE_ERROR`, 원본이 고장 나면 `502 ORIGIN_ERROR`예요. 캐시가 죽었다고 원본으로 몰래 우회하지 않아요. 캐시 값이나 원본 응답이 JSON이 아니어도 500 대신 같은 502예요. `item_id`가 `.`나 `..`이면 호출 없이 `400 INVALID_ITEM_ID`예요. |
| `GET /stats` | `{"hits": h, "misses": m}`를 돌려줘요. |
| `POST /stats/reset` | 두 카운터를 0으로 되돌려요. |

`SAMPLE_USE_CACHE=0`으로 띄우면 캐시를 전혀 건드리지 않아요(GET도 PUT도 없어요). 응답 헤더는 `X-Cache: BYPASS`이고 미스로 세어요. `0`이 아닌 값이나 없으면 캐시를 켜요. 캐시가 있을 때와 없을 때를 비교하는 `bench/cache_effect.py`가 이 모드를 써요.

## 단어장 시연 앱

단어장 앱 모양의 브라우저 화면이에요. 두 가지를 서로 다른 방식으로 빠르게 해요.

- **단어장 목록**: 서버는 DB에서 그냥 읽어 줘요. 빠르게 하는 일은 브라우저가 맡아요. 한 번 받은 목록을 `localStorage`에 저장해 두고, 단어장 탭에 다시 들어가도 서버를 부르지 않아요. 단어를 추가·삭제하거나 암기 상태를 바꾸면 서버에 쓰고 브라우저에 저장한 목록도 같이 고쳐요.
- **학습 통계**: 홈의 "오늘의 학습 현황"(오늘 학습 단어 수, 학습 시간, 연속 학습일)이에요. 학습 기록을 집계해야 나오는 값이라 Mini Redis에 cache-aside로 캐싱해요(키 `vocab:stats:demo`, TTL 60초). 학습 탭에서 플래시카드로 단어를 학습하면 기록을 DB에 쓰고 통계 키를 지워서, 다음에 홈을 열 때 다시 집계해요.

DB는 진짜 PostgreSQL이거나, 없으면 통계를 읽을 때 200ms를 기다려 느린 DB를 흉내 낸 메모리 DB예요. "오늘"은 한국 시간(UTC+9) 기준이에요.

    # Mini Redis(:8000)를 띄운 뒤
    uvicorn sample_app.vocab_app:app --port 8003                  # 메모리 DB(200ms 흉내)
    VOCAB_DB=pg uvicorn sample_app.vocab_app:app --port 8003      # 진짜 PostgreSQL
    # 브라우저에서 http://127.0.0.1:8003/

`VOCAB_DB=pg`는 `PG_DSN`(기본 `postgresql://postgres:bench@127.0.0.1:5433/bench`)의 PostgreSQL을 써요. 처음 한 번 `vocab_words`와 학습 기록 `vocab_study_events`를 만들고, 어제까지 4일 연속으로 학습한 약 30일치 기록을 넣어요(예전 시연이 만든 `vocab_study_logs`는 지워요). PostgreSQL을 띄우는 방법은 `bench/README.md`에 있어요.

`DB_RTT_MS`를 주면 "Redis는 앱 서버 가까이에, DB는 그보다 멀리 있다"고 가정하고 PostgreSQL을 읽고 쓸 때마다 그만큼의 왕복 시간(ms)을 대기로 더해요. Redis 호출에는 더하지 않아요(기본 0 = 더하지 않음, 메모리 DB에는 적용하지 않아요). `/api/state`의 `db_rtt_ms`에서 확인해요.

    DB_RTT_MS=30 VOCAB_DB=pg uvicorn sample_app.vocab_app:app --port 8003

`scripts/demo.sh`는 이 서버도 같이 띄워요(PostgreSQL이 떠 있으면 `VOCAB_DB=pg`와 `DB_RTT_MS=30`을 자동으로 넣어요. 끄려면 `DB_RTT_MS=0 ./scripts/demo.sh`). 접속 주소는 `MINI_REDIS_URL`(기본 `http://127.0.0.1:8000`)로 바꿔요.

| 엔드포인트 | 설명 |
| --- | --- |
| `GET /` | 시연 화면 |
| `GET /api/words` | 단어 목록. Mini Redis를 거치지 않고 DB에서 읽어요. |
| `POST /api/words` | `{"word":"cache","meaning":"[명사] 캐시"}`. 암기 미완료로 DB에 써요. 빈 단어는 422예요. |
| `PATCH /api/words/{id}` | `{"memorized": true}`. 암기 상태를 바꿔요. 없는 번호는 `404 WORD_NOT_FOUND`예요. |
| `DELETE /api/words/{id}` | DB에서 지워요. 없는 번호는 `404 WORD_NOT_FOUND`예요. |
| `POST /api/study` | `{"word_id": 1, "seconds": 8}`. 학습 기록을 DB에 쓰고 통계 캐시 키를 지워요. 없는 번호는 `404 WORD_NOT_FOUND`, 캐시를 못 지우면 `502 CACHE_ERROR`예요(DB에는 이미 반영돼요). |
| `GET /api/stats` | 오늘의 학습 현황 `{"stats": {"words_today", "seconds_today", "streak_days"}}`. 응답의 `cache`가 `HIT` / `MISS` / `BYPASS`(캐시 끔), `ms`가 걸린 시간이에요. 캐시가 고장 나면 `502 CACHE_ERROR`예요. |
| `GET /api/state` | 캐시에 든 키와 남은 TTL, 통계 조회 `hits` / `misses`, 통계를 DB에서 읽은 횟수 `db_reads`, 목록을 DB에서 읽은 횟수 `list_reads`, 쓰는 DB 종류 `db`(`postgres` 또는 `memory`)예요. |
| `POST /api/cache-mode` | `{"enabled": false}`면 통계 캐시를 쓰지 않고 매번 DB에서 집계해요(캐시 없던 때와 비교). |
| `POST /api/reset` | 단어장, 학습 기록, 집계, 캐시를 처음 상태로 되돌려요. |
