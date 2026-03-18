"""
q3_memgraph.py  — Full-text search for products by category keywords (Memgraph)
Assignment 2: Big Data Storage & Retrieval

Memgraph has no native tsvector; category_code is a dot-separated string
so CONTAINS gives exact word-level match for these keywords.

Same logic as q3_psql:
  - keywords: light, bicycle, refrigerators, tv
  - match categories where category_code CONTAINS keyword
  - rank products by global_purchases, top-5 per keyword
  - identifiers: product_sk, category_sk
"""

import os
from neo4j import GraphDatabase
from tabulate import tabulate

BOLT_URI = os.getenv("MEMGRAPH_URI", "bolt://localhost:7687")

CYPHER = """
WITH ['light', 'bicycle', 'refrigerators', 'tv'] AS keywords
UNWIND keywords AS keyword

MATCH (cat:Category)
WHERE cat.category_code CONTAINS keyword

MATCH (p:Product)-[:IN_CATEGORY]->(cat)

WITH keyword, p, cat,
     SIZE([(x)-[ev:HAD_EVENT {event_type: 'purchase'}]->(p) | ev]) AS global_purchases

ORDER BY keyword ASC, global_purchases DESC

WITH keyword,
     COLLECT({
         product_sk:       p.product_sk,
         product_id:       p.product_id,
         category_sk:      cat.category_sk,
         category_code:    cat.category_code,
         brand:            p.brand,
         price:            p.last_known_price,
         global_purchases: global_purchases
     })[..5] AS top5

UNWIND top5 AS rec
RETURN
    keyword,
    rec.product_sk       AS product_sk,
    rec.product_id       AS product_id,
    rec.category_sk      AS category_sk,
    rec.category_code    AS category_code,
    rec.brand            AS brand,
    rec.price            AS price,
    rec.global_purchases AS global_purchases
ORDER BY keyword ASC, global_purchases DESC;
"""

if __name__ == "__main__":
    driver = GraphDatabase.driver(BOLT_URI, auth=("", ""))
    with driver.session() as session:
        rows = [dict(r) for r in session.run(CYPHER)]
    driver.close()

    if not rows:
        print("No results.")
    else:
        headers = list(rows[0].keys())
        table   = [[r.get(h) for h in headers] for r in rows]
        print(tabulate(table, headers=headers, tablefmt="psql", floatfmt=".2f"))
        print(f"({len(rows)} rows)")
