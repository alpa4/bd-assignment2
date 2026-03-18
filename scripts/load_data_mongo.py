"""
load_data_mongo.py
Assignment 2: Big Data Storage & Retrieval

Loads cleaned CSV data into MongoDB (ecommerce database).

Collections:
  users     — _id: user_id; embedded: clients[], friends[]
  products  — _id: product_sk; product_id as regular field; embedded: category{}
  campaigns — _id: {campaign_id, campaign_type}
  messages  — _id: id; campaign: {campaign_id, message_type} as subdocument
  events    — _id: sequential; references product_sk (normalized)

Usage:
    python scripts/load_data_mongo.py
"""

import os
import sys
import pandas as pd
from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError

# ── config ───────────────────────────────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLEANED_DIR = os.path.join(BASE_DIR, "data", "cleaned")
MONGO_URI   = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME     = "ecommerce"
BATCH_SIZE  = 10_000


# ── helpers ──────────────────────────────────────────────────────────────────
def to_records(df: pd.DataFrame) -> list:
    """Convert DataFrame to list of dicts with NaN/NA → None."""
    return df.astype(object).where(pd.notna(df), other=None).to_dict("records")


def insert_batched(collection, docs: list) -> int:
    total = 0
    for i in range(0, len(docs), BATCH_SIZE):
        batch = docs[i : i + BATCH_SIZE]
        try:
            collection.insert_many(batch, ordered=False)
        except BulkWriteError:
            pass
        total += len(batch)
    return total


def log(name: str, count: int):
    print(f"  {name:<20} {count:>12,} documents inserted")


# ── loaders ──────────────────────────────────────────────────────────────────
def load_users(db):
    users_df   = pd.read_csv(os.path.join(CLEANED_DIR, "users.csv"))
    clients_df = pd.read_csv(
        os.path.join(CLEANED_DIR, "clients.csv"),
        dtype={"client_id": "int64", "user_id": "Int64", "user_device_id": "Int64"},
    )
    friends_df = pd.read_csv(os.path.join(CLEANED_DIR, "friends.csv"))

    # group clients by user_id — one-to-few (max ~9), safe to embed
    clients_by_user: dict = {}
    for r in to_records(clients_df):
        uid = int(r["user_id"])
        client = {"client_id": int(r["client_id"])}
        if r.get("user_device_id") is not None:
            client["user_device_id"] = int(r["user_device_id"])
        if r.get("first_purchase_date") is not None:
            client["first_purchase_date"] = str(r["first_purchase_date"])
        clients_by_user.setdefault(uid, []).append(client)

    # group friends by user_id (bidirectional)
    friends_df["friend1"] = friends_df["friend1"].astype("int64")
    friends_df["friend2"] = friends_df["friend2"].astype("int64")
    fwd = friends_df.groupby("friend1")["friend2"].apply(list).to_dict()
    bwd = friends_df.groupby("friend2")["friend1"].apply(list).to_dict()
    friends_by_user: dict = {}
    for uid, lst in fwd.items():
        friends_by_user[uid] = lst
    for uid, lst in bwd.items():
        friends_by_user.setdefault(uid, []).extend(lst)

    docs = []
    for r in to_records(users_df):
        uid = int(r["user_id"])
        doc: dict = {"_id": uid}
        clients = clients_by_user.get(uid)
        if clients:
            doc["clients"] = clients
        friends = friends_by_user.get(uid)
        if friends:
            doc["friends"] = friends
        docs.append(doc)

    db.users.create_index([("clients.client_id", ASCENDING)])
    count = insert_batched(db.users, docs)
    log("users", count)


def load_products(db):
    products_df   = pd.read_csv(os.path.join(CLEANED_DIR, "products.csv"),
                                dtype={"product_sk": "int64", "product_id": "int64",
                                       "category_sk": "Int64"})
    categories_df = pd.read_csv(os.path.join(CLEANED_DIR, "categories.csv"),
                                dtype={"category_sk": "int64", "category_id": "int64"})

    # category_sk → {category_id, category_code}
    cat_map = {
        int(r["category_sk"]): {
            "category_id":   int(r["category_id"]),
            "category_code": r["category_code"],
        }
        for r in to_records(categories_df)
    }

    docs = []
    for r in to_records(products_df):
        # _id = product_sk (unique per product × category version)
        doc: dict = {
            "_id":        int(r["product_sk"]),
            "product_id": int(r["product_id"]),
        }
        if r.get("category_sk") is not None:
            cat = cat_map.get(int(r["category_sk"]), {})
            subdoc = {}
            if cat.get("category_id") is not None:
                subdoc["category_id"] = cat["category_id"]
            if cat.get("category_code") is not None:
                subdoc["category_code"] = cat["category_code"]
            if subdoc:
                doc["category"] = subdoc
        if r.get("brand") is not None:
            doc["brand"] = r["brand"]
        if r.get("last_known_price") is not None:
            doc["last_known_price"] = float(r["last_known_price"])
        docs.append(doc)

    db.products.create_index([("product_id", ASCENDING)])
    db.products.create_index([("category.category_id", ASCENDING)])
    db.products.create_index([("brand", ASCENDING)])
    count = insert_batched(db.products, docs)
    log("products", count)


def load_campaigns(db):
    df = pd.read_csv(os.path.join(CLEANED_DIR, "campaigns.csv"))
    df = df.rename(columns={"id": "campaign_id"})
    df = df.drop_duplicates(["campaign_id", "campaign_type"])

    int_cols  = {"campaign_id", "total_count", "hour_limit", "subject_length"}
    bool_cols = {
        "ab_test", "warmup_mode", "subject_with_personalization",
        "subject_with_deadline", "subject_with_emoji", "subject_with_bonuses",
        "subject_with_discount", "subject_with_saleout", "is_test",
    }

    docs = []
    for r in to_records(df):
        cid   = int(r["campaign_id"])
        ctype = str(r["campaign_type"])
        # composite _id mirrors the composite PK in the relational model
        doc: dict = {"_id": {"campaign_id": cid, "campaign_type": ctype}}
        for k, v in r.items():
            if k in ("campaign_id", "campaign_type") or v is None:
                continue
            if k in int_cols:
                doc[k] = int(v)
            elif k in bool_cols:
                doc[k] = bool(v)
            else:
                doc[k] = v
        docs.append(doc)

    db.campaigns.create_index([("_id.campaign_id", ASCENDING)])
    db.campaigns.create_index([("channel", ASCENDING)])
    count = insert_batched(db.campaigns, docs)
    log("campaigns", count)


def load_messages(db):
    df = pd.read_csv(os.path.join(CLEANED_DIR, "messages.csv"), low_memory=False)

    bool_cols = {
        "is_opened", "is_clicked", "is_purchased", "is_unsubscribed",
        "is_hard_bounced", "is_soft_bounced", "is_complained", "is_blocked",
    }

    docs = []
    for r in to_records(df):
        doc: dict = {"_id": int(r["id"])}
        # campaign composite key as subdocument
        doc["campaign"] = {
            "campaign_id":  int(r["campaign_id"]),
            "message_type": r["message_type"],
        }
        for k, v in r.items():
            if k in ("id", "campaign_id", "message_type") or v is None:
                continue
            if k in bool_cols:
                doc[k] = bool(v)
            elif k == "client_id":
                doc[k] = int(v)
            else:
                doc[k] = v
        docs.append(doc)

    db.messages.create_index([("campaign.campaign_id", ASCENDING)])
    db.messages.create_index([("client_id", ASCENDING)])
    db.messages.create_index([("channel", ASCENDING)])
    db.messages.create_index([("sent_at", ASCENDING)])
    count = insert_batched(db.messages, docs)
    log("messages", count)


def load_events(db):
    df = pd.read_csv(os.path.join(CLEANED_DIR, "events.csv"),
                     dtype={"product_sk": "Int64"})
    # drop columns that live in the products collection
    drop_cols = ("category_id", "category_code", "brand", "product_id")
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    records = to_records(df)
    docs = []
    for event_id, r in enumerate(records, start=1):
        doc: dict = {"_id": event_id}
        for k, v in r.items():
            if v is not None:
                doc[k] = v
        docs.append(doc)

    db.events.create_index([("user_id", ASCENDING)])
    db.events.create_index([("product_sk", ASCENDING)])
    db.events.create_index([("event_type", ASCENDING)])
    db.events.create_index([("event_time", ASCENDING)])
    count = insert_batched(db.events, docs)
    log("events", count)


# ── main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 50)
    print("MongoDB data loader")
    print("=" * 50)

    print("\n[1/2] Connecting to MongoDB...")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
        print(f"  Connected to {MONGO_URI}")
    except Exception as e:
        print(f"  ERROR: {e}")
        sys.exit(1)

    db = client[DB_NAME]
    print(f"  Using database: {DB_NAME}")

    print(f"\n[2/2] Dropping database '{DB_NAME}'...")
    client.drop_database(DB_NAME)
    db = client[DB_NAME]
    print("  Done.")

    print("\n[3/3] Loading collections:")
    load_users(db)
    load_products(db)
    load_campaigns(db)
    load_messages(db)
    load_events(db)

    print("\nDocument counts in DB:")
    for col in ["users", "products", "campaigns", "messages", "events"]:
        print(f"  {col:<20} {db[col].count_documents({}):>12,}")

    print("\nDone.")
    client.close()
