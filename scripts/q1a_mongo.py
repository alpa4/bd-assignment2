"""
q1a.py  — Top campaigns by purchase conversion rate (MongoDB)
Assignment 2: Big Data Storage & Retrieval
"""

import os
from pymongo import MongoClient
from tabulate import tabulate

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.getenv("MONGO_DB", "ecommerce")

# channel is already a top-level field in each message — no $lookup needed
PIPELINE = [
    {"$group": {
        "_id": {
            "campaign_id":  "$campaign.campaign_id",
            "message_type": "$campaign.message_type",
        },
        "channel":    {"$first": "$channel"},
        "total_sent": {"$sum": 1},
        "opened":     {"$sum": {"$cond": [{"$eq": ["$is_opened",    True]}, 1, 0]}},
        "clicked":    {"$sum": {"$cond": [{"$eq": ["$is_clicked",   True]}, 1, 0]}},
        "purchased":  {"$sum": {"$cond": [{"$eq": ["$is_purchased", True]}, 1, 0]}},
    }},
    {"$addFields": {
        "purchase_rate_pct": {
            "$round": [
                {"$multiply": [
                    {"$divide": ["$purchased", "$total_sent"]},
                    100,
                ]},
                2,
            ]
        },
    }},
    {"$sort": {"purchase_rate_pct": -1}},
    {"$limit": 10},
    {"$project": {
        "_id":               0,
        "campaign_id":       "$_id.campaign_id",
        "campaign_type":     "$_id.message_type",
        "channel":           1,
        "total_sent":        1,
        "opened":            1,
        "clicked":           1,
        "purchased":         1,
        "purchase_rate_pct": 1,
    }},
]

if __name__ == "__main__":
    client = MongoClient(MONGO_URI)
    db     = client[DB_NAME]

    rows = list(db.messages.aggregate(PIPELINE))
    client.close()

    if not rows:
        print("No results.")
    else:
        headers = list(rows[0].keys())
        table   = [[r[h] for h in headers] for r in rows]
        print(tabulate(table, headers=headers, tablefmt="psql", floatfmt=".2f"))
        print(f"({len(rows)} rows)")
