#!/usr/bin/env bash
# 캐시 효과를 눈으로 보는 데모: Mini Redis + 가짜 원본(200ms) + 샘플 앱을 띄우고,
# 같은 요청을 두 번 보내 첫 번째(MISS)와 두 번째(HIT)의 속도 차이를 보여 준다.
#
#   ./scripts/demo.sh          서버를 켜 둔 채 Ctrl+C까지 기다린다
#   ./scripts/demo.sh --once   데모만 보여 주고 바로 끝낸다
#
# 포트는 환경 변수로 바꿀 수 있다: REDIS_PORT(8000) ORIGIN_PORT(8001) SAMPLE_PORT(8002)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PY="$ROOT/.venv/bin/python"
[ -x "$PY" ] || PY="${PYTHON:-python3}"
REDIS_PORT="${REDIS_PORT:-8000}"
ORIGIN_PORT="${ORIGIN_PORT:-8001}"
SAMPLE_PORT="${SAMPLE_PORT:-8002}"
ONCE=0
[ "${1:-}" = "--once" ] && ONCE=1

TMP="$(mktemp -d)"
PIDS=()
cleanup() {
  trap - EXIT INT TERM
  for pid in ${PIDS[@]+"${PIDS[@]}"}; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -rf "$TMP"
  echo "정리했어요. 서버 세 개를 모두 종료했습니다."
}
trap cleanup EXIT INT TERM

port_busy() { "$PY" -c "import socket,sys; s=socket.socket(); s.settimeout(0.3); sys.exit(0 if s.connect_ex(('127.0.0.1',$1))==0 else 1)"; }
for p in "$REDIS_PORT" "$ORIGIN_PORT" "$SAMPLE_PORT"; do
  if port_busy "$p"; then
    echo "포트 $p 를 이미 쓰고 있어요. 그 서버를 끄거나 REDIS_PORT/ORIGIN_PORT/SAMPLE_PORT로 다른 포트를 지정하세요." >&2
    exit 1
  fi
done

# 서버마다 --workers 1: 프로세스가 여러 개면 저장소가 따로 생긴다
start() { # 이름 앱 포트 [작업 폴더] -> 백그라운드로 띄운다
  local name="$1" app="$2" port="$3" dir="${4:-$ROOT}"
  (cd "$dir" && PYTHONPATH="$ROOT" \
     MINI_REDIS_URL="http://127.0.0.1:$REDIS_PORT" ORIGIN_URL="http://127.0.0.1:$ORIGIN_PORT" \
     exec "$PY" -m uvicorn "$app" --port "$port" --workers 1 --log-level warning \
     >"$TMP/$name.log" 2>&1) &
  PIDS+=("$!")
}
wait_ready() { # 이름 주소
  for _ in $(seq 1 50); do
    curl -sf "$2" >/dev/null 2>&1 && return 0
    sleep 0.2
  done
  echo "$1 가 뜨지 않았어요. 로그:" >&2; cat "$TMP/$1.log" >&2; exit 1
}

echo "서버를 띄우는 중..."
mkdir -p "$TMP/redis-data"   # Mini Redis의 영속 파일(data/)이 레포에 생기지 않게 임시 폴더에서 실행
start redis  mini_redis.api.app:app  "$REDIS_PORT"  "$TMP/redis-data"
start origin sample_app.origin:app   "$ORIGIN_PORT"
start sample sample_app.app:app      "$SAMPLE_PORT"
wait_ready redis  "http://127.0.0.1:$REDIS_PORT/health"
wait_ready origin "http://127.0.0.1:$ORIGIN_PORT/stats"
wait_ready sample "http://127.0.0.1:$SAMPLE_PORT/stats"

SAMPLE="http://127.0.0.1:$SAMPLE_PORT"
req() { # 아이템 id -> "X-Cache 걸린시간(ms)"
  local sec cache
  sec="$(curl -s -o /dev/null -D "$TMP/h" -w '%{time_total}' "$SAMPLE/items/$1")"
  cache="$(grep -i '^x-cache:' "$TMP/h" | tr -d '\r' | awk '{print $2}')"
  awk -v c="$cache" -v s="$sec" 'BEGIN{printf "%-5s %6.0f ms\n", c, s*1000}'
}

echo
echo "== 같은 아이템(7)을 3번 요청 =="
echo -n "1번째: "; req 7
echo -n "2번째: "; req 7
echo -n "3번째: "; req 7
echo
echo "== 다른 아이템(8) 1번, 그리고 7을 다시 =="
echo -n "아이템 8: "; req 8
echo -n "아이템 7: "; req 7
echo
echo "== 집계 =="
echo "샘플 앱  $(curl -s "$SAMPLE/stats")   (hits = 캐시에서 바로, misses = 원본까지 감)"
echo "원본 API $(curl -s "http://127.0.0.1:$ORIGIN_PORT/stats")   (원본이 실제로 불린 횟수)"
echo "캐시 키  $(curl -s "http://127.0.0.1:$REDIS_PORT/keys")"
echo "item:7 의 남은 TTL  $(curl -s "http://127.0.0.1:$REDIS_PORT/keys/item%3A7/ttl")  (초, 60초 뒤 만료)"

if [ "$ONCE" = 1 ]; then exit 0; fi
cat <<MSG

서버가 켜져 있어요. 직접 눌러 보세요 (끝내려면 Ctrl+C):
  샘플 앱     $SAMPLE/items/1        (X-Cache 헤더: curl -i)
  샘플 앱 문서 $SAMPLE/docs
  Mini Redis  http://127.0.0.1:$REDIS_PORT/docs
MSG
wait
