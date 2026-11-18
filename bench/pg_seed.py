"""벤치마크용 PostgreSQL 데이터를 만든다(이미 있으면 그대로 둔다).

  users:  1,000명 (기본키 id)
  orders: 2,000,000건 (user_id 인덱스 없음 → 사용자별 집계는 테이블 전체를 훑는다)

레포 루트에서:  python -m bench.pg_seed
"""
import os
import sys
import time

import psycopg

DSN = os.environ.get("PG_DSN", "postgresql://postgres:bench@127.0.0.1:5433/bench")
USERS, ORDERS = 1_000, 2_000_000


def main() -> None:
    with psycopg.connect(DSN, autocommit=True) as conn:
        exists = conn.execute("SELECT to_regclass('public.orders') IS NOT NULL").fetchone()[0]
        if exists:
            n = conn.execute("SELECT count(*) FROM orders").fetchone()[0]
            if n == ORDERS:
                print(f"이미 있어요: orders {n:,}건")
                return
            conn.execute("DROP TABLE orders")
        conn.execute("DROP TABLE IF EXISTS users")
        started = time.perf_counter()
        conn.execute("CREATE TABLE users (id int PRIMARY KEY, name text NOT NULL, email text NOT NULL)")
        conn.execute(
            "INSERT INTO users SELECT g, 'user' || g, 'user' || g || '@example.com' FROM generate_series(1, %s) g",
            (USERS,),
        )
        conn.execute("CREATE TABLE orders (id serial PRIMARY KEY, user_id int NOT NULL, category text NOT NULL, amount numeric(10,2) NOT NULL)")
        conn.execute(
            "INSERT INTO orders (user_id, category, amount) "
            "SELECT 1 + floor(random() * %s)::int, "
            "(ARRAY['food','book','game','music','travel','tech','home','sport'])[1 + (random() * 7)::int], "
            "round((random() * 100000)::numeric, 2) FROM generate_series(1, %s)",
            (USERS, ORDERS),
        )
        conn.execute("ANALYZE")
        print(f"만들었어요: users {USERS:,}명, orders {ORDERS:,}건 ({time.perf_counter() - started:.1f}초)", file=sys.stderr)


if __name__ == "__main__":
    main()
