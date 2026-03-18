"""
q1a.py  — Top campaigns by purchase conversion rate (Memgraph / Cypher)
Assignment 2: Big Data Storage & Retrieval
"""

import os
from neo4j import GraphDatabase
from tabulate import tabulate

BOLT_URI = os.getenv("MEMGRAPH_URI", "bolt://localhost:7687")

CYPHER = """
MATCH (c:Campaign)-[m:SENT_TO]->(cl:Client)
WITH
    c.campaign_id   AS campaign_id,
    c.campaign_type AS campaign_type,
    c.channel       AS channel,
    count(m)                                               AS total_sent,
    sum(CASE WHEN m.is_opened    = true THEN 1 ELSE 0 END) AS opened,
    sum(CASE WHEN m.is_clicked   = true THEN 1 ELSE 0 END) AS clicked,
    sum(CASE WHEN m.is_purchased = true THEN 1 ELSE 0 END) AS purchased
WITH
    campaign_id, campaign_type, channel,
    total_sent, opened, clicked, purchased,
    round(toFloat(purchased) / total_sent * 10000) / 100.0 AS purchase_rate_pct
ORDER BY purchase_rate_pct DESC
LIMIT 10
RETURN
    campaign_id, campaign_type, channel,
    total_sent, opened, clicked, purchased,
    purchase_rate_pct
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
        print(tabulate(table, headers=headers, tablefmt="psql", floatfmt=".2f"))
        print(f"({len(rows)} rows)")
