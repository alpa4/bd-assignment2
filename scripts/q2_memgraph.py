"""
q2_memgraph.py  — Personalised product recommendations (Memgraph / Cypher)
Assignment 2: Big Data Storage & Retrieval

Approach: 4 simple Cypher queries + pandas aggregation.
Single-query variant timed out (global_purchases computed per user×product).
"""

import os
import pandas as pd
from neo4j import GraphDatabase
from tabulate import tabulate

BOLT_URI = os.getenv("MEMGRAPH_URI", "bolt://localhost:7687")

# Q1: category scores per user
Q_CAT_SCORES = """
MATCH (u:User)-[e:HAD_EVENT]->(p:Product)-[:IN_CATEGORY]->(cat:Category)
RETURN
    u.user_id          AS user_id,
    cat.category_sk    AS category_sk,
    cat.category_code  AS category_code,
    SUM(CASE WHEN e.event_type = 'purchase' THEN 3
             WHEN e.event_type = 'cart'     THEN 2
             ELSE 1 END) AS cat_score
"""

# Q2: global purchase count per product (computed once, not per user)
Q_GLOBAL_POP = """
MATCH ()-[e:HAD_EVENT {event_type: 'purchase'}]->(p:Product)
RETURN
    p.product_sk        AS product_sk,
    p.product_id        AS product_id,
    p.brand             AS brand,
    p.last_known_price  AS price,
    COUNT(e)            AS global_purchases
"""

# Q3: which products belong to which category
Q_PROD_CAT = """
MATCH (p:Product)-[:IN_CATEGORY]->(cat:Category)
RETURN
    p.product_sk    AS product_sk,
    p.product_id    AS product_id,
    cat.category_sk AS category_sk
"""

# Q4: purchased product_sk per user (for exclusion filter — SK level, same as psql)
Q_PURCHASED = """
MATCH (u:User)-[e:HAD_EVENT {event_type: 'purchase'}]->(p:Product)
RETURN DISTINCT u.user_id AS user_id, p.product_sk AS product_sk
"""


def run(session, query):
    return pd.DataFrame([dict(r) for r in session.run(query)])


if __name__ == "__main__":
    driver = GraphDatabase.driver(BOLT_URI, auth=("", ""))
    with driver.session() as session:
        cat_scores  = run(session, Q_CAT_SCORES)
        global_pop  = run(session, Q_GLOBAL_POP)
        prod_cat    = run(session, Q_PROD_CAT)
        purchased   = run(session, Q_PURCHASED)
    driver.close()

    # --- top-3 categories per user ---
    cat_scores["rank"] = (
        cat_scores
        .groupby("user_id")["cat_score"]
        .rank(method="first", ascending=False)
    )
    top_cats = cat_scores[cat_scores["rank"] <= 3].copy()

    # --- candidates: product_sk × category_sk, with popularity attached ---
    # global_pop is per product_sk — no dedup needed (each SK is already unique)
    candidates = prod_cat.merge(
        global_pop[["product_sk", "brand", "price", "global_purchases"]],
        on="product_sk", how="left"
    )
    candidates["global_purchases"] = candidates["global_purchases"].fillna(0).astype(int)

    # --- purchased product_sk set per user ---
    if not purchased.empty:
        purchased_set = purchased.groupby("user_id")["product_sk"].apply(set).to_dict()
    else:
        purchased_set = {}

    # --- build recommendations ---
    results = []
    for _, row in top_cats.iterrows():
        uid      = row["user_id"]
        cat_sk   = row["category_sk"]
        cat_code = row["category_code"]

        already = purchased_set.get(uid, set())
        pool = candidates[
            (candidates["category_sk"] == cat_sk) &
            (~candidates["product_sk"].isin(already))
        ].sort_values("global_purchases", ascending=False).head(5)

        for _, p in pool.iterrows():
            results.append({
                "user_id":          uid,
                "category_code":    cat_code,
                "product_id":       int(p["product_id"]),
                "brand":            p["brand"],
                "price":            p["price"],
                "global_purchases": int(p["global_purchases"]),
            })

    df = (
        pd.DataFrame(results)
        .sort_values(["user_id", "global_purchases"], ascending=[True, False])
        .head(50)
    )

    if df.empty:
        print("No results.")
    else:
        print(tabulate(df, headers="keys", tablefmt="psql",
                       floatfmt=".2f", showindex=False))
        print(f"({len(df)} rows)")
