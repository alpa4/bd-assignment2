"""
q1b.py  — User targeting score: +2 if user purchased, +1 per purchaser friend
          (PostgreSQL own/denormalized model)
Assignment 2: Big Data Storage & Retrieval

Difference from 3NF: messages.user_id is stored directly —
no JOIN to clients needed to resolve user_id.
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
WITH purchasers AS (
    SELECT DISTINCT m.user_id
    FROM messages m
    WHERE m.is_purchased = TRUE
      AND m.user_id IS NOT NULL
),
friend_score AS (
    -- +1 for each friend who purchased (both directions of undirected friendship)
    SELECT f.user_id_2 AS user_id, COUNT(*) AS score
    FROM friendships f
    JOIN purchasers p ON f.user_id_1 = p.user_id
    GROUP BY 1

    UNION ALL

    SELECT f.user_id_1 AS user_id, COUNT(*) AS score
    FROM friendships f
    JOIN purchasers p ON f.user_id_2 = p.user_id
    GROUP BY 1
),
friend_score_agg AS (
    SELECT user_id, SUM(score) AS friend_purchasers
    FROM friend_score
    GROUP BY 1
)
SELECT
    fs.user_id,
    fs.friend_purchasers,
    CASE WHEN p.user_id IS NOT NULL THEN 2 ELSE 0 END  AS self_purchase_score,
    fs.friend_purchasers
        + CASE WHEN p.user_id IS NOT NULL THEN 2 ELSE 0 END AS total_score
FROM friend_score_agg fs
LEFT JOIN purchasers p ON fs.user_id = p.user_id
ORDER BY total_score DESC
LIMIT 20;
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
