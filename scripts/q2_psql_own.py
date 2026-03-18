"""
q2_psql_own.py  — Personalised product recommendations (PostgreSQL own/denormalised model)
Assignment 2: Big Data Storage & Retrieval

Difference vs 3NF model:
  - category_id, category_code, brand are stored inline in events → no JOIN to products/categories
    needed to compute category scores or retrieve category labels.
"""

import os
import psycopg2
from tabulate import tabulate

DB_HOST = os.getenv("PG_HOST", "localhost")
DB_PORT = int(os.getenv("PG_PORT", 5432))
DB_USER = os.getenv("PG_USER", "postgres")
DB_PASS = os.getenv("PG_PASS", "mysecretpassword")
DB_NAME = os.getenv("PG_DB",   "ecommerce_own")

SQL = """
WITH category_scores AS (
    -- category_sk is inline in events (own model) → direct grouping, no JOIN needed
    SELECT
        user_id,
        category_sk,
        SUM(CASE event_type
            WHEN 'purchase' THEN 3
            WHEN 'cart'     THEN 2
            ELSE 1
        END) AS cat_score
    FROM events
    GROUP BY user_id, category_sk
),
top_categories AS (
    SELECT
        user_id,
        category_sk,
        cat_score,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY cat_score DESC) AS rn
    FROM category_scores
),
product_popularity AS (
    SELECT
        product_sk,
        COUNT(*) FILTER (WHERE event_type = 'purchase') AS buy_count
    FROM events
    GROUP BY product_sk
),
top_products AS (
    SELECT
        p.product_id,
        p.product_sk,
        p.category_sk,
        p.brand,
        p.last_known_price,
        COALESCE(pp.buy_count, 0) AS buy_count,
        ROW_NUMBER() OVER (
            PARTITION BY p.category_sk
            ORDER BY COALESCE(pp.buy_count, 0) DESC
        ) AS prod_rn
    FROM products p
    LEFT JOIN product_popularity pp ON pp.product_sk = p.product_sk
),
already_purchased AS (
    -- exclude product versions (product_sk) the user already bought
    SELECT DISTINCT user_id, product_sk
    FROM events
    WHERE event_type = 'purchase'
)
SELECT
    tc.user_id,
    cat.category_code,
    tp.product_id,
    tp.brand,
    ROUND(tp.last_known_price::numeric, 2) AS price,
    tp.buy_count                           AS global_purchases
FROM top_categories    tc
JOIN top_products      tp  ON tp.category_sk  = tc.category_sk
JOIN categories        cat ON cat.category_sk = tc.category_sk
LEFT JOIN already_purchased ap
       ON ap.user_id = tc.user_id AND ap.product_sk = tp.product_sk
WHERE tc.rn      <= 3
  AND tp.prod_rn <= 5
  AND ap.product_sk IS NULL
ORDER BY tc.user_id, tc.cat_score DESC, tp.buy_count DESC
LIMIT 50;
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
