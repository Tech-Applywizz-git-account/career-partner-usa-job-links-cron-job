import os
import time
from datetime import datetime, timedelta, timezone


import pandas as pd
from sqlalchemy import create_engine, text
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

# =========================
# CONFIG (NEW)
# =========================

WINDOW_SIZE = timedelta(days=1)   # ✅ process 1 day per run
MAX_RUNTIME_SECONDS = 540         # ✅ 9 mins max (safe for 10 min cron)

CHUNK_SIZE = 2000                # ✅ reduced for speed
SUPABASE_BATCH = 200             # ✅ reduced for speed

start_execution = time.time()

# =========================
# CONNECTIONS
# =========================

try:
    conn_str = os.environ.get("PSQL_KEY")
    if not conn_str:
        raise ValueError("PSQL_KEY environment variable is missing")

    engine = create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_recycle=300
    )

    print("Successfully connected to PostgreSQL!")

except Exception as e:
    print(f"PostgreSQL connection error: {e}")
    exit()


try:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("SUPABASE_URL or SUPABASE_KEY missing")

    supabase = create_client(url, key)

    print("Successfully connected to Supabase!")

except Exception as e:
    print(f"Supabase connection error: {e}")
    exit()


# =========================
# CALCULATE PROCESSING WINDOW
# =========================

now = datetime.now(timezone.utc)

# We sync a rolling window of the last 2 days (48 hours) to ensure yesterday's 
# data is fully covered and we never miss any jobs (even if the cron job experiences 
# temporary delays or failures). Overlapping records are handled safely by Supabase's upsert on-conflict.
start_time = now - timedelta(days=2)
end_time = now

print("=" * 80)
print(f"Processing window: {start_time} → {end_time}")
print("=" * 80)

# =========================
# SQL QUERY (UPDATED)
# =========================

sql_query = f"""
SELECT
    j.id AS job_id,
    jr.id AS role_id,
    jr.name AS role_name,
    j.country_inferred AS country,
    j.location,
    j.title,
    j.salary,
    j.company AS company_name,
    j.url AS job_url,
    j.posted_by_profile,
    j.poster_full_name,
    j.description,
    j."datePosted" AS date_posted,
    j."uploadDate" AS upload_date

FROM "karmafy_job" j
LEFT JOIN "karmafy_jobrole" jr
    ON j."roleId"::bigint = jr.id

WHERE j."uploadDate" >= '{start_time}'
AND j."uploadDate" < '{end_time}'
AND j.country_inferred in ('United States of America','United States', 'US', 'USA')

ORDER BY j."uploadDate" ASC
"""


# =========================
# HELPERS
# =========================

def clean_value(v):
    if pd.isna(v):
        return None
    return v


def prepare_record(row):
    return {
        "job_id": int(row["job_id"]) if pd.notna(row["job_id"]) else None,
        "role_id": int(row["role_id"]) if pd.notna(row["role_id"]) else None,
        "role_name": clean_value(row["role_name"]),
        "indeed_search_country": clean_value(row["country"]),
        "country": clean_value(row["country"]),
        "salary": clean_value(row["salary"]),
        "location": clean_value(row["location"]),
        "title": clean_value(row["title"]),
        "company_name": clean_value(row["company_name"]),
        "job_url": clean_value(row["job_url"]),
        "job_url_direct": clean_value(row["job_url"]),
        "posted_by_profile": clean_value(row["posted_by_profile"]),
        "poster_full_name": clean_value(row["poster_full_name"]),
        "date_posted": row["date_posted"].isoformat() if pd.notna(row["date_posted"]) else None,
        "is_remote": None,
        "description": clean_value(row["description"]),
        "created_at": row["upload_date"].isoformat() if pd.notna(row["upload_date"]) else None,
        "source": "Karmafy"
    }


# =========================
# CHUNKED PROCESSING
# =========================

total_inserted = 0
total_errors = 0

print("=" * 80)
print("STARTING CHUNKED SYNC")
print("=" * 80)

try:
    for chunk_number, chunk_df in enumerate(
        pd.read_sql(text(sql_query), engine, chunksize=CHUNK_SIZE),
        start=1
    ):

        print(f"\nProcessing chunk {chunk_number} → {len(chunk_df)} rows")

        records = [prepare_record(row) for _, row in chunk_df.iterrows()]

        for i in range(0, len(records), SUPABASE_BATCH):

            # ✅ STOP if runtime exceeds limit
            if time.time() - start_execution > MAX_RUNTIME_SECONDS:
                print("⏱️ Max runtime reached, stopping early")
                raise Exception("Stopping early to avoid long cron execution")

            batch = records[i:i + SUPABASE_BATCH]

            try:
                response = (
                    supabase.table("jobs_all_roles")
                    .upsert(batch, on_conflict="job_url_direct")
                    .execute()
                )

                inserted = len(response.data) if response.data else 0
                total_inserted += inserted

                print(
                    f"✓ Chunk {chunk_number} | Batch {i//SUPABASE_BATCH + 1} | Inserted {inserted}"
                )

                time.sleep(0.1)  # ✅ prevent rate limits

            except Exception as batch_error:
                total_errors += len(batch)
                print(
                    f"✗ Chunk {chunk_number} | Batch {i//SUPABASE_BATCH + 1} | Error: {batch_error}"
                )

except Exception as e:
    print(f"Stopped safely: {e}")


# =========================
# FINAL REPORT
# =========================

print("\n" + "=" * 80)
print("SYNC COMPLETE")
print("=" * 80)
print(f"Inserted: {total_inserted}")
print(f"Errors: {total_errors}")
