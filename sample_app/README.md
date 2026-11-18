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

"단어장 메뉴에 들어갈 때마다 DB에서 목록을 다시 읽던" 상황을 보여 주는 브라우저 화면이에요. DB는 진짜 PostgreSQL이거나, 없으면 200ms 지연을 흉내 낸 메모리 DB예요.
화면은 단어장 앱 모양이에요. 하단 `단어장` 탭에 들어갈 때마다 목록을 Mini Redis에서 먼저 찾고(HIT), 없으면 DB에서 읽어 60초 캐시에 채워요(MISS). 단어를 추가하거나 지우면 DB에 쓴 뒤 캐시 키(`vocab:demo`)를 지워요.

    # Mini Redis(:8000)를 띄운 뒤
    uvicorn sample_app.vocab_app:app --port 8003                  # 메모리 DB(200ms 흉내)
    VOCAB_DB=pg uvicorn sample_app.vocab_app:app --port 8003      # 진짜 PostgreSQL
    # 브라우저에서 http://127.0.0.1:8003/

`VOCAB_DB=pg`는 `PG_DSN`(기본 `postgresql://postgres:bench@127.0.0.1:5433/bench`)의 PostgreSQL을 써요. 처음 한 번 `vocab_words`와 복습 기록 `vocab_study_logs` 200만 건을 만들어요. 목록을 읽을 때 이 복습 기록(인덱스 없음)을 집계해 단어별 복습 횟수를 같이 돌려줘요. PostgreSQL을 띄우는 방법은 `bench/README.md`에 있어요.

`scripts/demo.sh`는 이 서버도 같이 띄워요(PostgreSQL이 떠 있으면 `VOCAB_DB=pg`를 자동으로 넣어요). 접속 주소는 `MINI_REDIS_URL`(기본 `http://127.0.0.1:8000`)로 바꿔요.

| 엔드포인트 | 설명 |
| --- | --- |
| `GET /` | 시연 화면 |
| `GET /api/words` | 단어 목록. 응답의 `cache`가 `HIT` / `MISS` / `BYPASS`(캐시 끔), `ms`가 걸린 시간이에요. 캐시가 고장 나면 `502 CACHE_ERROR`예요. |
| `POST /api/words` | `{"word":"cache","meaning":"[명사] 캐시"}`. 암기 미완료로 DB에 쓰고 캐시 키를 지워요. 빈 단어는 422예요. |
| `PATCH /api/words/{id}` | `{"memorized": true}`. 암기 상태를 바꾸고 캐시 키를 지워요. 없는 번호는 `404 WORD_NOT_FOUND`예요. |
| `DELETE /api/words/{id}` | DB에서 지우고 캐시 키를 지워요. 없는 번호는 `404 WORD_NOT_FOUND`예요. |
| `GET /api/state` | 캐시에 든 키와 남은 TTL, `hits` / `misses` / `db_reads`, 쓰는 DB 종류 `db`(`postgres` 또는 `memory`)예요. |
| `POST /api/cache-mode` | `{"enabled": false}`면 캐시를 쓰지 않고 매번 DB를 읽어요(캐시 없던 때와 비교). |
| `POST /api/reset` | 단어장, 집계, 캐시를 처음 상태로 되돌려요. |
