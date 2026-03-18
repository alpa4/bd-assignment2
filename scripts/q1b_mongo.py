"""
q1b.py  — User targeting score: +2 if user purchased, +1 per purchaser friend (MongoDB)
Assignment 2: Big Data Storage & Retrieval

Data model notes:
  users: {_id: user_id, clients: [{client_id}], friends: [user_id, ...]}
  messages: {client_id, is_purchased, campaign: {...}}

Strategy:
  1. For each user, check if they purchased themselves (self_purchase_score = 2)
  2. Expand friends, check if each friend purchased (+1 per purchaser friend)
  3. Compute total_score = friend_purchasers + self_purchase_score
  4. Sort by total_score DESC, limit 20
"""

import os
from pymongo import MongoClient
from tabulate import tabulate

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.getenv("MONGO_DB", "ecommerce")

# Subpipeline: check if a user (by their client_ids) made any purchase
def _purchase_check_lookup(from_col, let_clients, as_field, limit=1):
    return {
        "from": from_col,
        "let": {
            "cids": {"$ifNull": [
                {"$map": {"input": let_clients, "as": "c", "in": "$$c.client_id"}},
                [],
            ]}
        },
        "pipeline": [
            {"$match": {"$expr": {"$and": [
                {"$in": ["$client_id", "$$cids"]},
                {"$eq": ["$is_purchased", True]},
            ]}}},
            {"$limit": limit},
        ],
        "as": as_field,
    }

PIPELINE = [
    # ── Step 1: tag every user with self_purchased ───────────────────────────
    {"$lookup": _purchase_check_lookup("messages", "$clients", "self_purchases")},
    {"$addFields": {
        "is_purchaser": {"$gt": [{"$size": "$self_purchases"}, 0]},
    }},

    # ── Step 2: only users who have at least one friend ──────────────────────
    {"$match": {
        "friends": {"$exists": True, "$not": {"$size": 0}},
    }},

    # ── Step 3: expand friend list ───────────────────────────────────────────
    {"$unwind": "$friends"},

    # ── Step 4: check if each friend is a purchaser ──────────────────────────
    {"$lookup": {
        "from": "users",
        "let":  {"fid": "$friends"},
        "pipeline": [
            {"$match": {"$expr": {"$eq": ["$_id", "$$fid"]}}},
            {"$lookup": _purchase_check_lookup("messages", "$clients", "purchases")},
            {"$project": {
                "is_purchaser": {"$gt": [{"$size": "$purchases"}, 0]}
            }},
        ],
        "as": "friend_doc",
    }},
    {"$unwind": "$friend_doc"},

    # ── Step 5: keep only pairs where friend is a purchaser ──────────────────
    {"$match": {"friend_doc.is_purchaser": True}},

    # ── Step 6: group by user, count purchaser friends ───────────────────────
    {"$group": {
        "_id":              "$_id",
        "is_purchaser":     {"$first": "$is_purchaser"},
        "friend_purchasers": {"$sum": 1},
    }},

    # ── Step 7: compute scores ───────────────────────────────────────────────
    {"$addFields": {
        "self_purchase_score": {"$cond": ["$is_purchaser", 2, 0]},
        "total_score": {"$add": [
            "$friend_purchasers",
            {"$cond": ["$is_purchaser", 2, 0]},
        ]},
    }},
    {"$sort":  {"total_score": -1}},
    {"$limit": 20},
    {"$project": {
        "_id":                0,
        "user_id":            "$_id",
        "friend_purchasers":  1,
        "self_purchase_score": 1,
        "total_score":        1,
    }},
]

if __name__ == "__main__":
    client = MongoClient(MONGO_URI)
    db     = client[DB_NAME]

    rows = list(db.users.aggregate(PIPELINE, allowDiskUse=True))
    client.close()

    if not rows:
        print("No results.")
    else:
        headers = list(rows[0].keys())
        table   = [[r[h] for h in headers] for r in rows]
        print(tabulate(table, headers=headers, tablefmt="psql"))
        print(f"({len(rows)} rows)")
