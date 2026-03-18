"""
q1b.py  — User targeting score: +2 if user purchased, +1 per purchaser friend (Memgraph)
Assignment 2: Big Data Storage & Retrieval

Graph model:
  (Campaign)-[:SENT_TO {is_purchased, ...}]->(Client)-[:BELONGS_TO]->(User)
  (User)-[:FRIENDS_WITH]-(User)   — undirected
"""

import os
from neo4j import GraphDatabase
from tabulate import tabulate

BOLT_URI = os.getenv("MEMGRAPH_URI", "bolt://localhost:7687")

CYPHER = """
// Step 1: find purchaser users via SENT_TO relationship
MATCH (:Campaign)-[m:SENT_TO]->(:Client)-[:BELONGS_TO]->(purchaser:User)
WHERE m.is_purchased = true
WITH collect(DISTINCT purchaser) AS purchasers

// Step 2: for each purchaser, find their friends (undirected)
UNWIND purchasers AS p
MATCH (p)-[:FRIENDS_WITH]-(candidate:User)

// Step 3: count how many purchaser friends each candidate has
WITH candidate, count(DISTINCT p) AS friend_purchasers

// Step 4: check if candidate themselves purchased
OPTIONAL MATCH (:Campaign)-[m2:SENT_TO]->(:Client)-[:BELONGS_TO]->(candidate)
WHERE m2.is_purchased = true

WITH
    candidate.user_id  AS user_id,
    friend_purchasers,
    count(m2)          AS self_purchase_count

WITH
    user_id,
    friend_purchasers,
    CASE WHEN self_purchase_count > 0 THEN 2 ELSE 0 END AS self_purchase_score

RETURN
    user_id,
    friend_purchasers,
    self_purchase_score,
    friend_purchasers + self_purchase_score AS total_score
ORDER BY total_score DESC
LIMIT 20
"""

if __name__ == "__main__":
    driver = GraphDatabase.driver(BOLT_URI, auth=("", ""))
    with driver.session() as session:
        result = session.run(CYPHER)
        rows   = [dict(r) for r in result]
    driver.close()

    if not rows:
        print("No results.")
    else:
        headers = list(rows[0].keys())
        table   = [[r[h] for h in headers] for r in rows]
        print(tabulate(table, headers=headers, tablefmt="psql"))
        print(f"({len(rows)} rows)")
