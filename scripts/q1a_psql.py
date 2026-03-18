"""
q1a.py  — Top campaigns by purchase conversion rate
Assignment 2: Big Data Storage & Retrieval
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
SELECT
    c.campaign_id,
    c.campaign_type,
    c.channel,
    COUNT(*)                                                   AS total_sent,
    SUM(m.is_opened::int)                                      AS opened,
    SUM(m.is_clicked::int)                                     AS clicked,
    SUM(m.is_purchased::int)                                   AS purchased,
    ROUND(100.0 * SUM(m.is_purchased::int) / COUNT(*), 2)     AS purchase_rate_pct
FROM messages m
JOIN campaigns c
  ON m.campaign_id  = c.campaign_id
 AND m.message_type = c.campaign_type
GROUP BY c.campaign_id, c.campaign_type, c.channel
ORDER BY purchase_rate_pct DESC
LIMIT 10;
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
