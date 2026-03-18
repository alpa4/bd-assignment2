"""
load_data_memgraph.py
Assignment 2: Big Data Storage & Retrieval

Loads cleaned CSV data into Memgraph (bolt://localhost:7687).

Graph model:
  Nodes:         User, Client, Product, Category, Campaign
  Relationships: BELONGS_TO, FRIENDS_WITH, IN_CATEGORY,
                 HAD_EVENT, SENT_TO

Usage:
    python scripts/load_data_memgraph.py
"""

import os
import sys
import pandas as pd
from neo4j import GraphDatabase

# ── config ───────────────────────────────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLEANED_DIR = os.path.join(BASE_DIR, "data", "cleaned")
BOLT_URI    = os.environ.get("MEMGRAPH_URI", "bolt://localhost:7687")
NODE_BATCH  = 2_000   # nodes: larger batches, no MATCH overhead
REL_BATCH   = 5_000   # relationships: larger batches reduce round-trips


# ── helpers ──────────────────────────────────────────────────────────────────
def to_records(df: pd.DataFrame) -> list:
    """Convert DataFrame to list of dicts with NaN/NA → None."""
    return df.astype(object).where(pd.notna(df), other=None).to_dict("records")


def run_batched(session, query: str, rows: list,
                label: str = "", batch_size: int = NODE_BATCH) -> int:
    """Run a parameterised Cypher query in batches.
    Each batch runs in its own committed transaction to avoid lock buildup."""
    total = len(rows)
    loaded = 0
    for i in range(0, total, batch_size):
        batch = rows[i : i + batch_size]
        with session.begin_transaction() as tx:
            tx.run(query, rows=batch)
            tx.commit()
        loaded += len(batch)
        if label and ((i // batch_size) % 5 == 0 or loaded == total):
            print(f"\r    {label}: {loaded:>10,} / {total:,}", end="", flush=True)
    if label:
        print()
    return loaded


# ── schema ───────────────────────────────────────────────────────────────────
SCHEMA_STATEMENTS = [
    # unique constraints (also create indexes automatically)
    "CREATE CONSTRAINT ON (u:User)       ASSERT u.user_id      IS UNIQUE",
    "CREATE CONSTRAINT ON (c:Client)     ASSERT c.client_id    IS UNIQUE",
    "CREATE CONSTRAINT ON (p:Product)    ASSERT p.product_sk   IS UNIQUE",
    "CREATE CONSTRAINT ON (cat:Category) ASSERT cat.category_sk IS UNIQUE",
    # explicit indexes for MATCH in relationship queries
    "CREATE INDEX ON :User(user_id)",
    "CREATE INDEX ON :Client(client_id)",
    "CREATE INDEX ON :Product(product_sk)",
    "CREATE INDEX ON :Product(product_id)",
    "CREATE INDEX ON :Category(category_sk)",
    "CREATE INDEX ON :Category(category_id)",
    "CREATE INDEX ON :Campaign(campaign_id)",
    "CREATE INDEX ON :Campaign(campaign_type)",
]


# ── node loaders ─────────────────────────────────────────────────────────────
def load_users(session):
    df = pd.read_csv(os.path.join(CLEANED_DIR, "users.csv"))
    rows = [{"user_id": int(r["user_id"])} for r in to_records(df)]
    run_batched(session,
        "UNWIND $rows AS row CREATE (:User {user_id: row.user_id})",
        rows, label="(:User)")


def load_clients(session):
    df = pd.read_csv(
        os.path.join(CLEANED_DIR, "clients.csv"),
        dtype={"client_id": "int64", "user_id": "Int64", "user_device_id": "Int64"},
    )
    rows = []
    for r in to_records(df):
        rows.append({
            "client_id":           int(r["client_id"]),
            "user_device_id":      int(r["user_device_id"]) if r["user_device_id"] is not None else None,
            "first_purchase_date": str(r["first_purchase_date"]) if r["first_purchase_date"] is not None else None,
        })
    run_batched(session,
        """UNWIND $rows AS row
           CREATE (:Client {
               client_id:           row.client_id,
               user_device_id:      row.user_device_id,
               first_purchase_date: row.first_purchase_date
           })""",
        rows, label="(:Client)")


def load_categories(session):
    df = pd.read_csv(os.path.join(CLEANED_DIR, "categories.csv"),
                     dtype={"category_sk": "int64", "category_id": "int64"})
    rows = [{"category_sk":   int(r["category_sk"]),
             "category_id":   int(r["category_id"]),
             "category_code": r["category_code"]}
            for r in to_records(df)]
    run_batched(session,
        """UNWIND $rows AS row
           CREATE (:Category {
               category_sk:   row.category_sk,
               category_id:   row.category_id,
               category_code: row.category_code
           })""",
        rows, label="(:Category)")


def load_products(session):
    df = pd.read_csv(os.path.join(CLEANED_DIR, "products.csv"),
                     dtype={"product_sk": "int64", "product_id": "int64",
                            "category_sk": "Int64"})
    rows = []
    for r in to_records(df):
        rows.append({
            "product_sk":       int(r["product_sk"]),
            "product_id":       int(r["product_id"]),
            "brand":            r["brand"],
            "last_known_price": float(r["last_known_price"]) if r["last_known_price"] is not None else None,
        })
    run_batched(session,
        """UNWIND $rows AS row
           CREATE (:Product {
               product_sk:       row.product_sk,
               product_id:       row.product_id,
               brand:            row.brand,
               last_known_price: row.last_known_price
           })""",
        rows, label="(:Product)")


def load_campaigns(session):
    df = pd.read_csv(os.path.join(CLEANED_DIR, "campaigns.csv"))
    df = df.rename(columns={"id": "campaign_id"})
    df = df.drop_duplicates(["campaign_id", "campaign_type"])

    rows = []
    for r in to_records(df):
        rows.append({
            "campaign_id":    int(r["campaign_id"]),
            "campaign_type":  r["campaign_type"],
            "channel":        r["channel"],
            "topic":          r["topic"],
            "started_at":     str(r["started_at"])  if r["started_at"]  is not None else None,
            "finished_at":    str(r["finished_at"]) if r["finished_at"] is not None else None,
            "position":       r["position"],
            "is_test":        bool(r["is_test"])      if r["is_test"]      is not None else None,
            "ab_test":        bool(r["ab_test"])      if r["ab_test"]      is not None else None,
            "warmup_mode":    bool(r["warmup_mode"])  if r["warmup_mode"]  is not None else None,
            "total_count":    int(r["total_count"])   if r["total_count"]  is not None else None,
            "hour_limit":     int(r["hour_limit"])    if r["hour_limit"]   is not None else None,
            "subject_length": int(r["subject_length"]) if r["subject_length"] is not None else None,
        })
    run_batched(session,
        """UNWIND $rows AS row
           CREATE (:Campaign {
               campaign_id:   row.campaign_id,
               campaign_type: row.campaign_type,
               channel:       row.channel,
               topic:         row.topic,
               started_at:    row.started_at,
               finished_at:   row.finished_at,
               total_count:   row.total_count,
               hour_limit:    row.hour_limit,
               subject_length: row.subject_length,
               ab_test:       row.ab_test,
               warmup_mode:   row.warmup_mode,
               is_test:       row.is_test,
               position:      row.position
           })""",
        rows, label="(:Campaign)")


# ── relationship loaders ──────────────────────────────────────────────────────
def load_belongs_to(session):
    df = pd.read_csv(
        os.path.join(CLEANED_DIR, "clients.csv"),
        dtype={"client_id": "int64", "user_id": "Int64"},
    )
    rows = [{"client_id": int(r["client_id"]), "user_id": int(r["user_id"])}
            for r in to_records(df) if r["user_id"] is not None]
    run_batched(session,
        """UNWIND $rows AS row
           MATCH (c:Client {client_id: row.client_id})
           MATCH (u:User   {user_id:   row.user_id})
           CREATE (c)-[:BELONGS_TO]->(u)""",
        rows, label="(:Client)-[:BELONGS_TO]->(:User)", batch_size=REL_BATCH)


def load_friends_with(session):
    df = pd.read_csv(os.path.join(CLEANED_DIR, "friends.csv"))
    rows = [{"u1": int(r["friend1"]), "u2": int(r["friend2"])}
            for r in to_records(df)]
    run_batched(session,
        """UNWIND $rows AS row
           MATCH (u1:User {user_id: row.u1})
           MATCH (u2:User {user_id: row.u2})
           CREATE (u1)-[:FRIENDS_WITH]->(u2)""",
        rows, label="(:User)-[:FRIENDS_WITH]->(:User)", batch_size=REL_BATCH)


def load_in_category(session):
    df = pd.read_csv(os.path.join(CLEANED_DIR, "products.csv"),
                     dtype={"product_sk": "int64", "category_sk": "Int64"})
    rows = [{"product_sk": int(r["product_sk"]), "category_sk": int(r["category_sk"])}
            for r in to_records(df) if r["category_sk"] is not None]
    run_batched(session,
        """UNWIND $rows AS row
           MATCH (p:Product  {product_sk:  row.product_sk})
           MATCH (c:Category {category_sk: row.category_sk})
           CREATE (p)-[:IN_CATEGORY]->(c)""",
        rows, label="(:Product)-[:IN_CATEGORY]->(:Category)", batch_size=REL_BATCH)


def load_had_event(session):
    df = pd.read_csv(os.path.join(CLEANED_DIR, "events.csv"),
                     dtype={"product_sk": "Int64"})
    drop = [c for c in ("category_id", "category_code", "brand", "product_id") if c in df.columns]
    if drop:
        df = df.drop(columns=drop)
    rows = []
    for r in to_records(df):
        if r["product_sk"] is None:
            continue
        rows.append({
            "user_id":      int(r["user_id"]),
            "product_sk":   int(r["product_sk"]),
            "event_type":   r["event_type"],
            "event_time":   str(r["event_time"]) if r["event_time"] is not None else None,
            "price":        float(r["price"]) if r["price"] is not None else None,
            "user_session": r["user_session"],
        })
    run_batched(session,
        """UNWIND $rows AS row
           MATCH (u:User    {user_id:    row.user_id})
           MATCH (p:Product {product_sk: row.product_sk})
           CREATE (u)-[:HAD_EVENT {
               event_type:   row.event_type,
               event_time:   row.event_time,
               price:        row.price,
               user_session: row.user_session
           }]->(p)""",
        rows, label="(:User)-[:HAD_EVENT]->(:Product)", batch_size=REL_BATCH)


def load_sent_to(session):
    """(Campaign)-[:SENT_TO {message props}]->(Client)
    Skips rows where client_id is null.
    """
    df = pd.read_csv(os.path.join(CLEANED_DIR, "messages.csv"), low_memory=False)
    bool_cols = {"is_opened", "is_clicked", "is_purchased", "is_unsubscribed",
                 "is_hard_bounced", "is_soft_bounced", "is_complained", "is_blocked"}
    rows = []
    for r in to_records(df):
        if r["client_id"] is None:
            continue
        row = {
            "campaign_id":    int(r["campaign_id"]),
            "message_type":   r["message_type"],
            "client_id":      int(r["client_id"]),
            "id":             int(r["id"]),
            "message_id":     r["message_id"],
            "channel":        r["channel"],
            "email_provider": r["email_provider"],
            "platform":       r["platform"],
            "stream":         r["stream"],
            "sent_at":        str(r["sent_at"]) if r["sent_at"] is not None else None,
        }
        for col in bool_cols:
            row[col] = bool(r[col]) if r[col] is not None else None
        rows.append(row)

    run_batched(session,
        """UNWIND $rows AS row
           MATCH (camp:Campaign {campaign_id: row.campaign_id, campaign_type: row.message_type})
           MATCH (cl:Client     {client_id:   row.client_id})
           CREATE (camp)-[:SENT_TO {
               id:              row.id,
               message_id:      row.message_id,
               message_type:    row.message_type,
               channel:         row.channel,
               email_provider:  row.email_provider,
               platform:        row.platform,
               stream:          row.stream,
               sent_at:         row.sent_at,
               is_opened:       row.is_opened,
               is_clicked:      row.is_clicked,
               is_purchased:    row.is_purchased,
               is_unsubscribed: row.is_unsubscribed,
               is_hard_bounced: row.is_hard_bounced,
               is_soft_bounced: row.is_soft_bounced,
               is_complained:   row.is_complained,
               is_blocked:      row.is_blocked
           }]->(cl)""",
        rows, label="(:Campaign)-[:SENT_TO]->(:Client)", batch_size=REL_BATCH)


# ── main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 50)
    print("Memgraph data loader")
    print("=" * 50)

    print(f"\n[1/4] Connecting to {BOLT_URI}...")
    driver = GraphDatabase.driver(BOLT_URI, auth=("", ""))
    try:
        driver.verify_connectivity()
        print("  Connected.")
    except Exception as e:
        print(f"  ERROR: {e}")
        sys.exit(1)

    with driver.session() as session:
        print("\n[2/4] Clearing database (DETACH DELETE + drop constraints)...")
        session.run("MATCH (n) DETACH DELETE n")
        # drop all existing constraints and indexes so re-runs with a new schema work
        for stmt in [
            "DROP CONSTRAINT ON (u:User)       ASSERT u.user_id      IS UNIQUE",
            "DROP CONSTRAINT ON (c:Client)     ASSERT c.client_id    IS UNIQUE",
            "DROP CONSTRAINT ON (p:Product)    ASSERT p.product_id   IS UNIQUE",
            "DROP CONSTRAINT ON (p:Product)    ASSERT p.product_sk   IS UNIQUE",
            "DROP CONSTRAINT ON (cat:Category) ASSERT cat.category_id IS UNIQUE",
            "DROP CONSTRAINT ON (cat:Category) ASSERT cat.category_sk IS UNIQUE",
        ]:
            try:
                session.run(stmt)
            except Exception:
                pass  # constraint didn't exist — that's fine
        print("  Done.")

        print("\n[3/4] Creating schema (constraints + indexes)...")
        for stmt in SCHEMA_STATEMENTS:
            session.run(stmt)
        print("  Done.")

        print("\n[4/4] Loading nodes:")
        load_users(session)
        load_clients(session)
        load_categories(session)
        load_products(session)
        load_campaigns(session)

        print("\n  Loading relationships:")
        load_belongs_to(session)
        load_friends_with(session)
        load_in_category(session)
        load_had_event(session)
        load_sent_to(session)

    print("\nSummary:")
    with driver.session() as session:
        print("  Nodes:")
        for label in ["User", "Client", "Product", "Category", "Campaign"]:
            c = session.run(f"MATCH (n:{label}) RETURN count(n) AS c").single()["c"]
            print(f"    {label:<15} {c:>10,}")
        print("  Relationships:")
        for rel in ["BELONGS_TO", "FRIENDS_WITH", "IN_CATEGORY", "HAD_EVENT", "SENT_TO"]:
            c = session.run(f"MATCH ()-[r:{rel}]->() RETURN count(r) AS c").single()["c"]
            print(f"    {rel:<25} {c:>10,}")

    driver.close()
    print("\nDone.")
