"""
q3_mongo.py  — Full-text search for products by category keywords (MongoDB)
Assignment 2: Big Data Storage & Retrieval

Single aggregation pipeline on products collection.

Same logic as q3_psql:
  - keywords: light, bicycle, refrigerators, tv
  - find products whose category_code contains the keyword (regex)
  - rank by global_purchases (purchase events per product_sk)
  - return top-5 per keyword

Note: category_sk is not stored in MongoDB products (only category_id +
category_code are embedded). product_sk = _id field.
"""

import os
from pymongo import MongoClient
from tabulate import tabulate

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.getenv("MONGO_DB",  "ecommerce")

KEYWORDS = ["light", "bicycle", "refrigerators", "tv"]

PIPELINE = [
    # ── Step 1: filter products matching any keyword ───────────────────────────
    {"$match": {
        "category.category_code": {
            "$regex": r"(^|\.)(" + "|".join(KEYWORDS) + r")(\.|$)",
            "$options": "i",
        }
    }},

    # ── Step 2: tag product with matching keyword(s) ───────────────────────────
    {"$addFields": {
        "matching_keywords": {
            "$filter": {
                "input": KEYWORDS,
                "as":    "kw",
                "cond": {"$regexMatch": {
                    "input":   "$category.category_code",
                    "regex":   {"$concat": ["(^|\\.)", "$$kw", "(\\.|$)"]},
                    "options": "i",
                }},
            }
        }
    }},

    # ── Step 3: count global purchases per product_sk from events ──────────────
    {"$lookup": {
        "from": "events",
        "let":  {"psk": "$_id"},
        "pipeline": [
            {"$match": {"$expr": {"$and": [
                {"$eq": ["$product_sk",  "$$psk"]},
                {"$eq": ["$event_type",  "purchase"]},
            ]}}},
            {"$count": "cnt"},
        ],
        "as": "purchase_docs",
    }},
    {"$addFields": {
        "global_purchases": {"$ifNull": [{"$first": "$purchase_docs.cnt"}, 0]}
    }},

    # ── Step 4: one row per (product, keyword) ─────────────────────────────────
    {"$unwind": "$matching_keywords"},

    # ── Step 5: sort then group → top-5 per keyword ────────────────────────────
    {"$sort": {"matching_keywords": 1, "global_purchases": -1}},
    {"$group": {
        "_id": "$matching_keywords",
        "top5": {"$push": {
            "product_sk":       "$_id",
            "product_id":       "$product_id",
            "category_code":    "$category.category_code",
            "brand":            "$brand",
            "price":            "$last_known_price",
            "global_purchases": "$global_purchases",
        }},
    }},
    {"$project": {"top5": {"$slice": ["$top5", 5]}}},

    # ── Step 6: flatten and project ───────────────────────────────────────────
    {"$unwind": "$top5"},
    {"$project": {
        "_id":              0,
        "keyword":          "$_id",
        "product_sk":       "$top5.product_sk",
        "product_id":       "$top5.product_id",
        "category_code":    "$top5.category_code",
        "brand":            "$top5.brand",
        "price":            "$top5.price",
        "global_purchases": "$top5.global_purchases",
    }},
    {"$sort": {"keyword": 1, "global_purchases": -1}},
]

if __name__ == "__main__":
    client = MongoClient(MONGO_URI)
    db     = client[DB_NAME]

    rows = list(db.products.aggregate(PIPELINE, allowDiskUse=True))
    client.close()

    if not rows:
        print("No results.")
    else:
        headers = list(rows[0].keys())
        table   = [[r.get(h) for h in headers] for r in rows]
        print(tabulate(table, headers=headers, tablefmt="psql", floatfmt=".2f"))
        print(f"({len(rows)} rows)")
