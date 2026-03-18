"""
q2_mongo.py  — Personalised product recommendations (MongoDB)
Assignment 2: Big Data Storage & Retrieval

Approach: 4 separate aggregation pipelines + pandas (same as q2_memgraph).
Single-pipeline correlated $lookup variant took ~5500s — avoided here.

Pipelines:
  1. cat_scores  : (user_id, category_id, category_code) → cat_score
  2. global_pop  : product_sk → global purchase count
  3. prod_cat    : product_sk → product_id, brand, price, category_id, category_code
  4. purchased   : (user_id, product_sk) pairs where event_type = 'purchase'

Pandas:
  - top-3 categories per user (by cat_score)
  - join candidates (prod_cat × global_pop)
  - exclude already-purchased product_sk per user
  - top-5 products per (user, category) by global_purchases
  - limit 50 rows
"""

import os
import pandas as pd
from pymongo import MongoClient
from tabulate import tabulate

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.getenv("MONGO_DB",  "ecommerce")

# ── Pipeline 1: category interest score per user ──────────────────────────────
PIPELINE_CAT_SCORES = [
    {"$project": {
        "user_id":    1,
        "product_sk": 1,
        "event_type": 1,
        "weight": {"$switch": {
            "branches": [
                {"case": {"$eq": ["$event_type", "purchase"]}, "then": 3},
                {"case": {"$eq": ["$event_type", "cart"]},     "then": 2},
            ],
            "default": 1,
        }},
    }},
    {"$group": {
        "_id":   {"user_id": "$user_id", "product_sk": "$product_sk"},
        "score": {"$sum": "$weight"},
    }},
    {"$lookup": {
        "from":         "products",
        "localField":   "_id.product_sk",
        "foreignField": "_id",
        "as":           "product",
    }},
    {"$unwind": "$product"},
    {"$group": {
        "_id": {
            "user_id":       "$_id.user_id",
            "category_id":   "$product.category.category_id",
            "category_code": "$product.category.category_code",
        },
        "cat_score": {"$sum": "$score"},
    }},
    {"$project": {
        "_id":           0,
        "user_id":       "$_id.user_id",
        "category_id":   "$_id.category_id",
        "category_code": "$_id.category_code",
        "cat_score":     1,
    }},
]

# ── Pipeline 2: global purchase count per product_sk ──────────────────────────
PIPELINE_GLOBAL_POP = [
    {"$match": {"event_type": "purchase"}},
    {"$group": {
        "_id":              "$product_sk",
        "global_purchases": {"$sum": 1},
    }},
    {"$project": {
        "_id":              0,
        "product_sk":       "$_id",
        "global_purchases": 1,
    }},
]

# ── Pipeline 3: product catalogue (from products collection) ──────────────────
PIPELINE_PROD_CAT = [
    {"$project": {
        "_id":           0,
        "product_sk":    "$_id",
        "product_id":    1,
        "brand":         1,
        "price":         "$last_known_price",
        "category_id":   "$category.category_id",
        "category_code": "$category.category_code",
    }},
]

# ── Pipeline 4: purchased (user_id, product_sk) pairs ─────────────────────────
PIPELINE_PURCHASED = [
    {"$match": {"event_type": "purchase"}},
    {"$group": {
        "_id": {"user_id": "$user_id", "product_sk": "$product_sk"},
    }},
    {"$project": {
        "_id":        0,
        "user_id":    "$_id.user_id",
        "product_sk": "$_id.product_sk",
    }},
]


def agg(collection, pipeline):
    return pd.DataFrame(list(collection.aggregate(pipeline, allowDiskUse=True)))


if __name__ == "__main__":
    client = MongoClient(MONGO_URI)
    db     = client[DB_NAME]

    cat_scores = agg(db.events,   PIPELINE_CAT_SCORES)
    global_pop = agg(db.events,   PIPELINE_GLOBAL_POP)
    prod_cat   = agg(db.products, PIPELINE_PROD_CAT)
    purchased  = agg(db.events,   PIPELINE_PURCHASED)

    client.close()

    # ── top-3 categories per user ──────────────────────────────────────────────
    cat_scores["rank"] = (
        cat_scores
        .groupby("user_id")["cat_score"]
        .rank(method="first", ascending=False)
    )
    top_cats = cat_scores[cat_scores["rank"] <= 3].copy()

    # ── candidates: products with popularity attached ──────────────────────────
    candidates = prod_cat.merge(
        global_pop[["product_sk", "global_purchases"]],
        on="product_sk", how="left"
    )
    candidates["global_purchases"] = candidates["global_purchases"].fillna(0).astype(int)

    # ── purchased product_sk set per user ──────────────────────────────────────
    if not purchased.empty:
        purchased_set = (
            purchased.groupby("user_id")["product_sk"]
            .apply(set).to_dict()
        )
    else:
        purchased_set = {}

    # ── build recommendations ──────────────────────────────────────────────────
    results = []
    for _, row in top_cats.iterrows():
        uid      = row["user_id"]
        cat_id   = row["category_id"]
        cat_code = row["category_code"]

        already = purchased_set.get(uid, set())

        # NaN-safe category match: pandas NaN == NaN is False, use isna() for nulls
        if pd.isna(cat_id):
            mask_cat  = candidates["category_id"].isna()
        else:
            mask_cat  = candidates["category_id"] == cat_id

        if pd.isna(cat_code):
            mask_code = candidates["category_code"].isna()
        else:
            mask_code = candidates["category_code"] == cat_code

        pool = candidates[
            mask_cat & mask_code &
            (~candidates["product_sk"].isin(already))
        ].sort_values("global_purchases", ascending=False).head(5)

        for _, p in pool.iterrows():
            results.append({
                "user_id":          uid,
                "category_code":    cat_code,
                "product_id":       int(p["product_id"]),
                "brand":            p["brand"] if pd.notna(p["brand"]) else None,
                "price":            p["price"] if pd.notna(p["price"]) else None,
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
