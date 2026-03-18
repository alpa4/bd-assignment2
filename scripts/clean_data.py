"""
clean_data.py
Assignment 2: Big Data Storage & Retrieval

Reads the 5 raw CSV files, cleans and normalises them,
and writes the results to assignment2/data/cleaned/.

Usage:
    python scripts/clean_data.py
"""

import os
import re
import pandas as pd

# ── paths ───────────────────────────────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR     = os.path.join(BASE_DIR, "data", "raw")
CLEANED_DIR = os.path.join(BASE_DIR, "data", "cleaned")
os.makedirs(CLEANED_DIR, exist_ok=True)


# ── helpers ─────────────────────────────────────────────────────────────────
def report(name: str, df_raw: pd.DataFrame, df_clean: pd.DataFrame) -> None:
    """Print a short cleaning summary."""
    dropped = len(df_raw) - len(df_clean)
    print(f"[{name}]  raw={len(df_raw):>8,}  cleaned={len(df_clean):>8,}  dropped={dropped:>6,}")


def normalize_str(series: pd.Series) -> pd.Series:
    """Lowercase + strip a string column, preserving NaN as NULL.
    Works even when the column is entirely NaN (pandas reads it as float64)."""
    return series.astype(str).where(series.notna()).str.strip().str.lower()


def parse_bool_column(series: pd.Series) -> pd.Series:
    """Convert 't'/'f', 'True'/'False', 1/0 → Python bool (nullable)."""
    mapping = {
        "t": True,  "f": False,
        "true": True, "false": False,
        "1": True, "0": False,
    }
    return (
        series.astype(str)
              .str.strip()
              .str.lower()
              .map(mapping)
    )


# ── 1. events.csv ────────────────────────────────────────────────────────────
def clean_events() -> pd.DataFrame:
    df = pd.read_csv(os.path.join(RAW_DIR, "events.csv"))

    # --- timestamps ---
    df["event_time"] = pd.to_datetime(
        df["event_time"].str.replace(" UTC", "", regex=False),
        utc=True
    )

    # --- normalise event_type ---
    df["event_type"] = normalize_str(df["event_type"])
    #valid_types = {"view", "cart", "purchase", "remove_from_cart"}
    #df = df[df["event_type"].isin(valid_types)]

    # --- category_code: fill missing with '' ---
    df["category_code"] = df["category_code"].str.strip()

    # --- brand: lowercase, strip, fill missing ---
    df["brand"] = normalize_str(df["brand"])

    # --- price: drop rows where price is missing or negative ---
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df[df["price"].notna() & (df["price"] >= 0)]

    # --- product_id / user_id must be present ---
    df = df.dropna(subset=["product_id", "user_id"])
    df["product_id"] = df["product_id"].astype("int64")
    df["user_id"]    = df["user_id"].astype("int64")
    df["category_id"] = df["category_id"].astype("int64")

    # --- session: strip whitespace ---
    df["user_session"] = df["user_session"].str.strip()

    # --- deduplicate ---
    df = df.drop_duplicates()

    report("events", pd.read_csv(os.path.join(RAW_DIR, "events.csv")), df)
    df.to_csv(os.path.join(CLEANED_DIR, "events.csv"), index=False)
    return df


# ── 2. campaigns.csv ─────────────────────────────────────────────────────────
def clean_campaigns() -> pd.DataFrame:
    df = pd.read_csv(os.path.join(RAW_DIR, "campaigns.csv"))

    # --- timestamps ---
    for col in ["started_at", "finished_at"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # --- campaign_type / channel ---
    df["campaign_type"] = normalize_str(df["campaign_type"])
    df["channel"]       = normalize_str(df["channel"])
    df["topic"]         = normalize_str(df["topic"])

    # --- boolean columns ---
    bool_cols = [
        "ab_test", "warmup_mode",
        "subject_with_personalization", "subject_with_deadline",
        "subject_with_emoji", "subject_with_bonuses",
        "subject_with_discount", "subject_with_saleout", "is_test",
    ]
    for col in bool_cols:
        if col in df.columns:
            df[col] = parse_bool_column(df[col])

    # --- numeric ---
    df["hour_limit"]      = pd.to_numeric(df["hour_limit"],      errors="coerce")
    df["subject_length"]  = pd.to_numeric(df["subject_length"],  errors="coerce")
    df["total_count"]     = pd.to_numeric(df["total_count"],     errors="coerce").astype("Int64")

    # --- position: normalise ---
    df["position"] = normalize_str(df["position"])

    # --- required fields ---
    df = df.dropna(subset=["id", "campaign_type"])
    df["id"] = df["id"].astype("int64")

    df = df.drop_duplicates(subset=["id", "campaign_type"])

    report("campaigns", pd.read_csv(os.path.join(RAW_DIR, "campaigns.csv")), df)
    df.to_csv(os.path.join(CLEANED_DIR, "campaigns.csv"), index=False)
    return df


# ── 3. messages.csv ──────────────────────────────────────────────────────────
def clean_messages() -> pd.DataFrame:
    df = pd.read_csv(os.path.join(RAW_DIR, "messages.csv"), low_memory=False)

    # --- boolean columns (stored as 't'/'f') ---
    bool_flags = [
        "is_opened", "is_clicked", "is_purchased",
        "is_unsubscribed", "is_hard_bounced", "is_soft_bounced",
        "is_complained", "is_blocked",
    ]
    for col in bool_flags:
        df[col] = parse_bool_column(df[col])

    # --- timestamp columns ---
    ts_cols = [
        "sent_at",
        "opened_first_time_at", "opened_last_time_at",
        "clicked_first_time_at", "clicked_last_time_at",
        "purchased_at",
        "unsubscribed_at", "hard_bounced_at", "soft_bounced_at",
        "complained_at", "blocked_at",
        "created_at", "updated_at",
    ]
    for col in ts_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    # --- categorical: strip/lowercase, preserve NaN as NULL ---
    for col in ["message_type", "channel", "category", "platform",
                "email_provider", "stream"]:
        df[col] = normalize_str(df[col])

    # --- ids ---
    # id is the integer PK (surrogate key, required)
    df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["id"])
    df["id"] = df["id"].astype("int64")

    # message_id is the business key (UUID or "{client_id}-{hex}")
    df = df.dropna(subset=["message_id"])
    df = df[df["message_id"].astype(str).str.strip() != ""]

    # Only campaign_id and message_type are truly required (bulk has no client_id)
    df = df.dropna(subset=["campaign_id", "message_type"])
    df["campaign_id"] = df["campaign_id"].astype("int64")
    df["client_id"]   = pd.to_numeric(df["client_id"], errors="coerce").astype("Int64")
    df["user_id"]     = pd.to_numeric(df["user_id"], errors="coerce").astype("Int64")
    df["user_device_id"] = pd.to_numeric(df["user_device_id"], errors="coerce").astype("Int64")

    df = df.drop_duplicates(subset=["id"])

    report("messages", pd.read_csv(os.path.join(RAW_DIR, "messages.csv"), low_memory=False), df)
    df.to_csv(os.path.join(CLEANED_DIR, "messages.csv"), index=False)
    return df


# ── 4. clients (messages + client_first_purchase_date.csv) ───────────────────
def clean_clients(messages_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a complete clients table:
      - All clients who ever received a message (from messages.csv)
        → client_id, user_id, user_device_id
      - Left-joined with client_first_purchase_date.csv
        → first_purchase_date is NULL for clients who never purchased
    """
    # ── clients from messages ────────────────────────────────────────────────
    from_messages = (
        messages_df[["client_id", "user_id", "user_device_id"]]
        .dropna(subset=["client_id"])
        .copy()
    )
    from_messages["client_id"]      = from_messages["client_id"].astype("int64")
    from_messages["user_id"]        = pd.to_numeric(from_messages["user_id"],        errors="coerce").astype("Int64")
    from_messages["user_device_id"] = pd.to_numeric(from_messages["user_device_id"], errors="coerce").astype("Int64")
    from_messages["first_purchase_date"] = pd.NaT

    # ── clients from client_first_purchase_date.csv ──────────────────────────
    fpd = pd.read_csv(os.path.join(RAW_DIR, "client_first_purchase_date.csv"),
                      dtype={"client_id": "int64"})
    fpd["first_purchase_date"] = pd.to_datetime(
        fpd["first_purchase_date"], errors="coerce"
    ).dt.date
    fpd["user_id"]        = pd.to_numeric(fpd["user_id"],        errors="coerce").astype("Int64")
    fpd["user_device_id"] = pd.to_numeric(fpd["user_device_id"], errors="coerce").astype("Int64")
    fpd = fpd[["client_id", "user_id", "user_device_id", "first_purchase_date"]]

    # ── concat → sort so rows WITH first_purchase_date come first ────────────
    # drop_duplicates keeps the first occurrence → keeps the purchase date row
    clients = (
        pd.concat([fpd, from_messages], ignore_index=True)
        .drop_duplicates(subset=["client_id"], keep="first")
        .sort_values("client_id")
        .reset_index(drop=True)
    )

    raw_count = len(fpd)
    print(f"[clients]  total={len(clients):>8,}  with purchase_date={clients['first_purchase_date'].notna().sum():>8,}  (fpd only={raw_count:,})")
    clients.to_csv(os.path.join(CLEANED_DIR, "clients.csv"), index=False)
    return clients


# ── 5. friends.csv ───────────────────────────────────────────────────────────
def clean_friends() -> pd.DataFrame:
    df = pd.read_csv(os.path.join(RAW_DIR, "friends.csv"))

    df = df.dropna(subset=["friend1", "friend2"])
    df["friend1"] = df["friend1"].astype("int64")
    df["friend2"] = df["friend2"].astype("int64")

    # Remove self-loops
    df = df[df["friend1"] != df["friend2"]]

    # Normalise direction so friend1 < friend2 (undirected dedup)
    df[["friend1", "friend2"]] = pd.DataFrame(
        df[["friend1", "friend2"]].apply(sorted, axis=1).tolist(),
        index=df.index
    )

    df = df.drop_duplicates()

    report("friends", pd.read_csv(os.path.join(RAW_DIR, "friends.csv")), df)
    df.to_csv(os.path.join(CLEANED_DIR, "friends.csv"), index=False)
    return df


# ── 6. Derived helper tables ──────────────────────────────────────────────────
def extract_products_and_categories(events_df: pd.DataFrame) -> None:
    """Build categories.csv and products.csv with surrogate keys (SCD-style),
    and re-save events.csv with a product_sk column so each event resolves to
    the category that was *current at event time* (not the last-known one).
    """

    # 1. categories: unique (category_id, category_code) pairs → category_sk
    cats = (
        events_df[["category_id", "category_code"]]
        .drop_duplicates()
        .sort_values(["category_id", "category_code"])
        .reset_index(drop=True)
    )
    cats.insert(0, "category_sk", range(1, len(cats) + 1))
    cats.to_csv(os.path.join(CLEANED_DIR, "categories.csv"), index=False)
    print(f"[categories] extracted {len(cats):,} unique (category_id, category_code) pairs")

    # 2. products: unique (product_id, category_id, category_code) → product_sk
    #    brand and last_known_price = last value by event_time in each group
    prod_base = (
        events_df[["product_id", "category_id", "category_code", "brand", "price", "event_time"]]
        .sort_values("event_time")
        .groupby(["product_id", "category_id", "category_code"], dropna=False)
        .agg(brand=("brand", "last"), last_known_price=("price", "last"))
        .reset_index()
    )
    prod_base.insert(0, "product_sk", range(1, len(prod_base) + 1))

    # attach category_sk
    prod_with_cat = prod_base.merge(
        cats[["category_sk", "category_id", "category_code"]],
        on=["category_id", "category_code"],
        how="left",
    )
    products = prod_with_cat[["product_sk", "product_id", "category_sk",
                               "brand", "last_known_price"]]
    products.to_csv(os.path.join(CLEANED_DIR, "products.csv"), index=False)
    print(f"[products]   extracted {len(products):,} unique (product_id, category) variants")

    # 3. re-save events.csv with product_sk added
    key_map = prod_base[["product_sk", "product_id", "category_id", "category_code"]]
    events_with_sk = events_df.merge(
        key_map, on=["product_id", "category_id", "category_code"], how="left"
    )
    null_sk = int(events_with_sk["product_sk"].isna().sum())
    events_with_sk.to_csv(os.path.join(CLEANED_DIR, "events.csv"), index=False)
    print(f"[events]     re-saved with product_sk ({null_sk:,} unmatched)")


def extract_users(events_df: pd.DataFrame, clients_df: pd.DataFrame,
                  friends_df: pd.DataFrame) -> None:
    """Build users.csv combining all user_id sources.
    messages is excluded: every message recipient has a client_id → user_id
    in clients, so clients already covers all message recipients.
    """
    user_ids = pd.concat([
        events_df[["user_id"]],
        clients_df[["user_id"]],
        friends_df[["friend1"]].rename(columns={"friend1": "user_id"}),
        friends_df[["friend2"]].rename(columns={"friend2": "user_id"}),
    ], ignore_index=True)

    users = (
        user_ids.drop_duplicates(subset=["user_id"])
                .sort_values("user_id")
                .reset_index(drop=True)
    )
    users.to_csv(os.path.join(CLEANED_DIR, "users.csv"), index=False)
    print(f"[users]      extracted {len(users):,} unique users")


# ── 7. Referential integrity filter ──────────────────────────────────────────
def enforce_referential_integrity(campaigns_df: pd.DataFrame,
                                  messages_df:  pd.DataFrame,
                                  clients_df:   pd.DataFrame) -> None:
    """
    Cross-table FK filtering so all three databases (PostgreSQL, MongoDB,
    Memgraph) receive exactly the same dataset.

    Rules applied:
      1. messages: keep only rows whose (campaign_id, message_type) exists
         in campaigns.

    Overwrites messages.csv in the cleaned directory.
    """
    print("\n[referential integrity]")

    # ── 1: filter by campaign FK ─────────────────────────────────────────────
    camp_keys = set(
        zip(campaigns_df["id"].astype(int),
            campaigns_df["campaign_type"].astype(str))
    )

    before = len(messages_df)
    messages_df = messages_df[
        messages_df.apply(
            lambda r: (int(r["campaign_id"]), str(r["message_type"])) in camp_keys,
            axis=1
        )
    ]
    dropped_camp = before - len(messages_df)

    print(f"  messages: dropped {dropped_camp:>8,} with unknown campaign FK")
    print(f"  messages: {len(messages_df):>10,} rows kept")
    messages_df.to_csv(os.path.join(CLEANED_DIR, "messages.csv"), index=False)


# ── main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("Cleaning data…")
    print("=" * 60)

    events_df    = clean_events()
    campaigns_df = clean_campaigns()
    messages_df  = clean_messages()
    clients_df   = clean_clients(messages_df)
    friends_df   = clean_friends()

    print("-" * 60)
    extract_products_and_categories(events_df)
    extract_users(events_df, clients_df, friends_df)

    print("-" * 60)
    enforce_referential_integrity(campaigns_df, messages_df, clients_df)

    print("=" * 60)
    print(f"Cleaned files saved to: {CLEANED_DIR}")
