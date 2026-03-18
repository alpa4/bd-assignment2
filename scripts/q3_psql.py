"""
q3_psql.py  — Full-text search for products by category keywords (PostgreSQL)
Assignment 2: Big Data Storage & Retrieval

Keywords: last word from top Q2 category_codes:
  construction.tools.light        → light
  sport.bicycle                   → bicycle
  appliances.kitchen.refrigerators → refrigerators
  electronics.video.tv            → tv

For each keyword:
  1. Full-text search on category_code (dots replaced with spaces)
  2. Find all products in matching categories
  3. Rank by global_purchases (buy_count from events)
  4. Return top-5 per keyword

Identifiers: product_sk, category_sk throughout.
"""

import os
import psycopg2
from tabulate import tabulate

DB_HOST = os.getenv("PG_HOST", "localhost")
DB_PORT = int(os.getenv("PG_PORT", 5432))
DB_USER = os.getenv("PG_USER", "postgres")
DB_PASS = os.getenv("PG_PASS", "mysecretpassword")
DB_NAME = os.getenv("PG_DB",   "ecommerce")

SQL = """
WITH global_pop AS (
    SELECT product_sk,
           COUNT(*) FILTER (WHERE event_type = 'purchase') AS buy_count
    FROM events
    GROUP BY product_sk
),
search_results AS (
    SELECT
        k.keyword,
        p.product_sk,
        p.product_id,
        c.category_sk,
        c.category_code,
        p.brand,
        p.last_known_price,
        COALESCE(gp.buy_count, 0) AS global_purchases,
        ROW_NUMBER() OVER (
            PARTITION BY k.keyword
            ORDER BY COALESCE(gp.buy_count, 0) DESC
        ) AS rn
    FROM (VALUES ('light'), ('bicycle'), ('refrigerators'), ('tv')) AS k(keyword)
    JOIN categories c
      ON to_tsvector('simple', REPLACE(COALESCE(c.category_code, ''), '.', ' '))
         @@ to_tsquery('simple', k.keyword)
    JOIN products p ON p.category_sk = c.category_sk
    LEFT JOIN global_pop gp ON gp.product_sk = p.product_sk
)
SELECT
    keyword,
    product_sk,
    product_id,
    category_sk,
    category_code,
    brand,
    ROUND(last_known_price::numeric, 2) AS price,
    global_purchases
FROM search_results
WHERE rn <= 5
ORDER BY keyword, global_purchases DESC;
"""

if __name__ == "__main__":
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    with conn.cursor() as cur:
        cur.execute(SQL)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    conn.close()

    print(tabulate(rows, headers=cols, tablefmt="psql", floatfmt=".2f"))
    print(f"({len(rows)} rows)")
