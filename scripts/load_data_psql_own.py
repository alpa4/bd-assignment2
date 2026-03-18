"""
load_data_psql_own.py
Assignment 2: Big Data Storage & Retrieval

Loads cleaned CSV data into the denormalised "own" PostgreSQL model
(database: ecommerce_own).

Key differences from the 3NF model (load_data_psql.py):
  - messages: PK is message_id TEXT; stores user_id + user_device_id
    directly (denormalised from clients — avoids JOIN to users).
  - events:   no table partitioning; stores brand, category_id,
    category_code inline (denormalised from products/categories).

Usage:
    python scripts/load_data_psql_own.py
"""

import io
import os
import sys

import pandas as pd
import psycopg2
from psycopg2 import sql

# ── connection config ────────────────────────────────────────────────────────
DB_HOST = os.getenv("PG_HOST", "localhost")
DB_PORT = int(os.getenv("PG_PORT", 5432))
DB_USER = os.getenv("PG_USER", "postgres")
DB_PASS = os.getenv("PG_PASS", "mysecretpassword")
DB_NAME = os.getenv("PG_DB",   "ecommerce_own")   # separate DB from 3NF model

# ── paths ────────────────────────────────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLEANED_DIR = os.path.join(BASE_DIR, "data", "cleaned")


# ── helpers ──────────────────────────────────────────────────────────────────
def connect(dbname=DB_NAME):
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=dbname, user=DB_USER, password=DB_PASS
    )


def copy_df(conn, df: pd.DataFrame, table: str) -> int:
    """Bulk-load a DataFrame into a table using COPY (fastest method)."""
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    cols = ", ".join(df.columns)
    with conn.cursor() as cur:
        cur.copy_expert(
            f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
            buf
        )
    conn.commit()
    return len(df)


def row_count(conn, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]


def log(table: str, n: int):
    print(f"  {table:<20} {n:>10,} rows loaded")


# ── schema ───────────────────────────────────────────────────────────────────
SCHEMA_SQL = """
DROP TABLE IF EXISTS friendships   CASCADE;
DROP TABLE IF EXISTS messages      CASCADE;
DROP TABLE IF EXISTS events        CASCADE;
DROP TABLE IF EXISTS campaigns     CASCADE;
DROP TABLE IF EXISTS clients       CASCADE;
DROP TABLE IF EXISTS products      CASCADE;
DROP TABLE IF EXISTS categories    CASCADE;
DROP TABLE IF EXISTS users         CASCADE;

-- ── dimension tables ────────────────────────────────────────────────────────

CREATE TABLE users (
    user_id  BIGINT  PRIMARY KEY
);

CREATE TABLE clients (
    client_id           BIGINT   PRIMARY KEY,
    user_id             BIGINT   NOT NULL REFERENCES users(user_id),
    user_device_id      INTEGER,
    first_purchase_date DATE,
    UNIQUE (user_id, user_device_id)
);

CREATE TABLE categories (
    category_sk   BIGINT        PRIMARY KEY,
    category_id   BIGINT        NOT NULL,
    category_code VARCHAR(255),
    UNIQUE (category_id, category_code)
);

CREATE TABLE products (
    product_sk       BIGINT          PRIMARY KEY,
    product_id       BIGINT          NOT NULL,
    category_sk      BIGINT          REFERENCES categories(category_sk),
    brand            VARCHAR(100),
    last_known_price DECIMAL(10, 2)
);

-- ── marketing tables ────────────────────────────────────────────────────────

CREATE TABLE campaigns (
    campaign_id                   INTEGER      NOT NULL,
    campaign_type                 VARCHAR(50)  NOT NULL,
    channel                       VARCHAR(50),
    topic                         VARCHAR(255),
    started_at                    TIMESTAMP,
    finished_at                   TIMESTAMP,
    total_count                   BIGINT,
    ab_test                       BOOLEAN,
    warmup_mode                   BOOLEAN  DEFAULT FALSE,
    hour_limit                    NUMERIC,
    subject_length                INTEGER,
    subject_with_personalization  BOOLEAN  DEFAULT FALSE,
    subject_with_deadline         BOOLEAN  DEFAULT FALSE,
    subject_with_emoji            BOOLEAN  DEFAULT FALSE,
    subject_with_bonuses          BOOLEAN  DEFAULT FALSE,
    subject_with_discount         BOOLEAN  DEFAULT FALSE,
    subject_with_saleout          BOOLEAN  DEFAULT FALSE,
    is_test                       BOOLEAN  DEFAULT FALSE,
    position                      VARCHAR(100),
    PRIMARY KEY (campaign_id, campaign_type)
);

-- Denormalised: stores user_id + user_device_id directly
-- (avoids JOIN messages → clients → users on reads)
CREATE TABLE messages (
    id                    INTEGER      NOT NULL PRIMARY KEY,
    message_id            TEXT         UNIQUE,
    campaign_id           INTEGER      NOT NULL,
    message_type          VARCHAR(50)  NOT NULL,
    client_id             BIGINT       REFERENCES clients(client_id),
    user_id               BIGINT       REFERENCES users(user_id),
    user_device_id        INTEGER,
    channel               VARCHAR(50),
    platform              VARCHAR(50),
    email_provider        VARCHAR(100),
    stream                VARCHAR(50),
    date                  DATE,
    sent_at               TIMESTAMP,
    is_opened             BOOLEAN  DEFAULT FALSE,
    opened_first_time_at  TIMESTAMP,
    opened_last_time_at   TIMESTAMP,
    is_clicked            BOOLEAN  DEFAULT FALSE,
    clicked_first_time_at  TIMESTAMP,
    clicked_last_time_at   TIMESTAMP,
    is_purchased          BOOLEAN  DEFAULT FALSE,
    purchased_at          TIMESTAMP,
    is_unsubscribed       BOOLEAN  DEFAULT FALSE,
    unsubscribed_at       TIMESTAMP,
    is_hard_bounced       BOOLEAN  DEFAULT FALSE,
    hard_bounced_at       TIMESTAMP,
    is_soft_bounced       BOOLEAN  DEFAULT FALSE,
    soft_bounced_at       TIMESTAMP,
    is_complained         BOOLEAN  DEFAULT FALSE,
    complained_at         TIMESTAMP,
    is_blocked            BOOLEAN  DEFAULT FALSE,
    blocked_at            TIMESTAMP,
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP,
    FOREIGN KEY (campaign_id, message_type)
        REFERENCES campaigns(campaign_id, campaign_type)
);

-- Denormalised: brand, category_id, category_code stored inline
-- (avoids JOIN events → products → categories on reads)
CREATE TABLE events (
    event_id      BIGSERIAL,
    event_time    TIMESTAMP WITH TIME ZONE  NOT NULL,
    event_type    VARCHAR(20)               NOT NULL,
    product_sk    BIGINT                    REFERENCES products(product_sk),
    product_id    BIGINT,
    user_id       BIGINT                    REFERENCES users(user_id),
    user_session  UUID,
    price         DECIMAL(10, 2),
    brand         VARCHAR(100),
    category_id   BIGINT,
    category_code VARCHAR(255),
    category_sk   BIGINT                    REFERENCES categories(category_sk),
    PRIMARY KEY (event_id, event_time)
) PARTITION BY RANGE (event_time);

CREATE TABLE events_2019_10 PARTITION OF events
    FOR VALUES FROM ('2019-10-01') TO ('2019-11-01');
CREATE TABLE events_2019_11 PARTITION OF events
    FOR VALUES FROM ('2019-11-01') TO ('2019-12-01');
CREATE TABLE events_2019_12 PARTITION OF events
    FOR VALUES FROM ('2019-12-01') TO ('2020-01-01');

-- ── social graph ─────────────────────────────────────────────────────────────

CREATE TABLE friendships (
    user_id_1  BIGINT  NOT NULL  REFERENCES users(user_id),
    user_id_2  BIGINT  NOT NULL  REFERENCES users(user_id),
    PRIMARY KEY (user_id_1, user_id_2),
    CONSTRAINT friendship_order CHECK (user_id_1 < user_id_2)
);

-- ── indexes ──────────────────────────────────────────────────────────────────

CREATE INDEX idx_events_user_id     ON events (user_id);
CREATE INDEX idx_events_product_sk  ON events (product_sk);
CREATE INDEX idx_events_product_id  ON events (product_id);
CREATE INDEX idx_events_type        ON events (event_type);
CREATE INDEX idx_events_session     ON events (user_session);
CREATE INDEX idx_events_category_id ON events (category_id);
CREATE INDEX idx_events_category_sk ON events (category_sk);
CREATE INDEX idx_events_brand       ON events (brand);

CREATE INDEX idx_messages_campaign  ON messages (campaign_id, message_type);
CREATE INDEX idx_messages_client    ON messages (client_id);
CREATE INDEX idx_messages_user      ON messages (user_id);
CREATE INDEX idx_messages_user_dev  ON messages (user_id, user_device_id);
CREATE INDEX idx_messages_channel   ON messages (channel);
CREATE INDEX idx_messages_date      ON messages (date);
CREATE INDEX idx_messages_purchased ON messages (is_purchased) WHERE is_purchased = TRUE;

CREATE INDEX idx_products_category  ON products (category_sk);
CREATE INDEX idx_products_brand     ON products (brand);
CREATE INDEX idx_clients_user_id    ON clients (user_id);
CREATE INDEX idx_friendships_user2  ON friendships (user_id_2);
"""


def create_schema(conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()
    print("  Schema created.")


# ── loaders (in FK dependency order) ─────────────────────────────────────────

def load_users(conn):
    df = pd.read_csv(os.path.join(CLEANED_DIR, "users.csv"))
    df = df[["user_id"]].drop_duplicates()
    log("users", copy_df(conn, df, "users"))


def load_categories(conn):
    df = pd.read_csv(os.path.join(CLEANED_DIR, "categories.csv"),
                     dtype={"category_sk": "int64", "category_id": "int64"})
    df = df[["category_sk", "category_id", "category_code"]]
    log("categories", copy_df(conn, df, "categories"))


def load_products(conn):
    df = pd.read_csv(os.path.join(CLEANED_DIR, "products.csv"),
                     dtype={"product_sk": "int64", "product_id": "int64",
                            "category_sk": "Int64"})
    df = df[["product_sk", "product_id", "category_sk", "brand", "last_known_price"]]
    log("products", copy_df(conn, df, "products"))


def load_clients(conn):
    df = pd.read_csv(os.path.join(CLEANED_DIR, "clients.csv"),
                     dtype={"client_id": "int64", "user_id": "Int64",
                            "user_device_id": "Int64"})
    df = df[["client_id", "user_id", "user_device_id", "first_purchase_date"]]
    df = df.drop_duplicates("client_id")
    log("clients", copy_df(conn, df, "clients"))


def load_campaigns(conn):
    df = pd.read_csv(os.path.join(CLEANED_DIR, "campaigns.csv"))
    df = df.rename(columns={"id": "campaign_id"})

    cols = [
        "campaign_id", "campaign_type", "channel", "topic",
        "started_at", "finished_at", "total_count",
        "ab_test", "warmup_mode", "hour_limit",
        "subject_length", "subject_with_personalization",
        "subject_with_deadline", "subject_with_emoji",
        "subject_with_bonuses", "subject_with_discount",
        "subject_with_saleout", "is_test", "position",
    ]
    df = df[cols].drop_duplicates(["campaign_id", "campaign_type"])

    for col in ["total_count", "hour_limit", "subject_length"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    log("campaigns", copy_df(conn, df, "campaigns"))


def load_events(conn):
    df = pd.read_csv(os.path.join(CLEANED_DIR, "events.csv"),
                     dtype={"product_sk": "Int64", "category_id": "Int64"})

    # derive category_sk from products.csv (product_sk → category_sk)
    prod_map = pd.read_csv(
        os.path.join(CLEANED_DIR, "products.csv"),
        dtype={"product_sk": "int64", "category_sk": "Int64"},
        usecols=["product_sk", "category_sk"],
    )
    df = df.merge(prod_map, on="product_sk", how="left")

    with conn.cursor() as cur:
        cur.execute("SELECT user_id FROM users")
        user_ids = {row[0] for row in cur.fetchall()}
        cur.execute("SELECT product_sk FROM products")
        product_sks = {row[0] for row in cur.fetchall()}

    before = len(df)
    df = df[df["user_id"].isin(user_ids) & df["product_sk"].isin(product_sks)]
    if len(df) < before:
        print(f"    (skipped {before - len(df)} events with unresolved FKs)")

    df["event_id"] = range(1, len(df) + 1)

    cols = [
        "event_id", "event_time", "event_type",
        "product_sk", "product_id", "user_id", "user_session", "price",
        "brand", "category_id", "category_code", "category_sk",
    ]
    cols = [c for c in cols if c in df.columns]
    log("events", copy_df(conn, df[cols], "events"))


def load_messages(conn):
    df = pd.read_csv(os.path.join(CLEANED_DIR, "messages.csv"), low_memory=False,
                     dtype={"client_id": "Int64", "user_id": "Int64",
                            "user_device_id": "Int64"})

    # resolve composite FK → campaigns
    with conn.cursor() as cur:
        cur.execute("SELECT campaign_id, campaign_type FROM campaigns")
        camp_keys = {(row[0], row[1]) for row in cur.fetchall()}
    before = len(df)
    df = df[df.apply(
        lambda r: (int(r["campaign_id"]), str(r["message_type"])) in camp_keys, axis=1
    )]
    if len(df) < before:
        print(f"    (skipped {before - len(df)} messages with unknown campaign FK)")

    # resolve FK → clients (NULL client_id is allowed)
    with conn.cursor() as cur:
        cur.execute("SELECT client_id FROM clients")
        client_ids = {row[0] for row in cur.fetchall()}
    before = len(df)
    df = df[df["client_id"].isna() | df["client_id"].isin(client_ids)]
    if len(df) < before:
        print(f"    (skipped {before - len(df)} messages with unknown client_id)")

    # user_id and user_device_id come directly from messages.csv (denormalised)
    cols = [
        "id", "message_id", "campaign_id", "message_type",
        "client_id", "user_id", "user_device_id",
        "channel", "platform", "email_provider", "stream",
        "date", "sent_at",
        "is_opened", "opened_first_time_at", "opened_last_time_at",
        "is_clicked", "clicked_first_time_at", "clicked_last_time_at",
        "is_purchased", "purchased_at",
        "is_unsubscribed", "unsubscribed_at",
        "is_hard_bounced", "hard_bounced_at",
        "is_soft_bounced", "soft_bounced_at",
        "is_complained", "complained_at",
        "is_blocked", "blocked_at",
        "created_at", "updated_at",
    ]
    cols = [c for c in cols if c in df.columns]
    log("messages", copy_df(conn, df[cols], "messages"))


def load_friendships(conn):
    df = pd.read_csv(os.path.join(CLEANED_DIR, "friends.csv"))
    df = df.rename(columns={"friend1": "user_id_1", "friend2": "user_id_2"})

    with conn.cursor() as cur:
        cur.execute("SELECT user_id FROM users")
        user_ids = {row[0] for row in cur.fetchall()}
    before = len(df)
    df = df[df["user_id_1"].isin(user_ids) & df["user_id_2"].isin(user_ids)]
    if len(df) < before:
        print(f"    (skipped {before - len(df)} friendships with unknown user_id)")

    log("friendships", copy_df(conn, df[["user_id_1", "user_id_2"]], "friendships"))


# ── main ─────────────────────────────────────────────────────────────────────

def ensure_db_exists():
    try:
        conn = connect(dbname="postgres")
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
            if not cur.fetchone():
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
                print(f"  Database '{DB_NAME}' created.")
            else:
                print(f"  Database '{DB_NAME}' already exists.")
        conn.close()
    except Exception as e:
        print(f"  Could not ensure DB exists: {e}")
        sys.exit(1)


def check_cleaned_data():
    required = ["users.csv", "categories.csv", "products.csv",
                "clients.csv", "campaigns.csv",
                "events.csv", "messages.csv", "friends.csv"]
    missing = [f for f in required if not os.path.exists(os.path.join(CLEANED_DIR, f))]
    if missing:
        print("ERROR: cleaned data not found. Run clean_data.py first.")
        for f in missing:
            print(f"  missing: {f}")
        sys.exit(1)


if __name__ == "__main__":
    print("=" * 50)
    print("PostgreSQL OWN model data loader")
    print("=" * 50)

    check_cleaned_data()

    print("\n[1/3] Connecting to PostgreSQL...")
    ensure_db_exists()
    conn = connect()
    print(f"  Connected to {DB_HOST}:{DB_PORT}/{DB_NAME}")

    print("\n[2/3] Creating schema...")
    create_schema(conn)

    print("\n[3/3] Loading data (FK order):")
    load_users(conn)
    load_categories(conn)
    load_products(conn)
    load_clients(conn)
    load_campaigns(conn)
    load_events(conn)
    load_messages(conn)
    load_friendships(conn)

    print("\nRow counts in DB:")
    for t in ["users", "clients", "categories", "products",
              "campaigns", "events", "messages", "friendships"]:
        print(f"  {t:<20} {row_count(conn, t):>10,}")

    conn.close()
    print("\nDone.")
