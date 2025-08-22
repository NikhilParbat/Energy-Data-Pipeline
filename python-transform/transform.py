import os
import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = os.environ["DATABASE_URL"]
engine = create_engine(DB_URL, future=True)

def ensure_region_dim(conn, regions):
    # insert if not exists, return mapping name -> id
    mapping = {}
    for r in regions:
        conn.execute(text("""
            INSERT INTO region_dim (region_name)
            VALUES (:name)
            ON CONFLICT (region_name) DO NOTHING;
        """), {"name": r})
    res = conn.execute(text("SELECT region_id, region_name FROM region_dim WHERE region_name = ANY(:arr)"),
                       {"arr": list(regions)})
    for row in res.fetchall():
        mapping[row.region_name] = row.region_id
    return mapping

def ensure_datetime_dim(conn, timestamps):
    # bulk upsert datetime_dim for all ts in set
    if not timestamps:
        return
    rows = []
    for ts in timestamps:
        ts_utc = pd.to_datetime(ts, utc=True)
        rows.append({
            "ts": ts_utc,
            "date": ts_utc.date(),
            "hour": ts_utc.hour,
            "dow": ts_utc.dayofweek,
            "month": ts_utc.month,
            "year": ts_utc.year,
            "is_weekend": ts_utc.dayofweek >= 5
        })
    df = pd.DataFrame(rows)
    with conn.begin():
        # temp table approach for speed
        conn.exec_driver_sql("CREATE TEMP TABLE tmp_datetime_dim AS SELECT * FROM datetime_dim WITH NO DATA;")
        df.to_sql("tmp_datetime_dim", conn.connection, if_exists="append", index=False)
        conn.execute(text("""
            INSERT INTO datetime_dim (ts, date, hour, dow, month, year, is_weekend)
            SELECT DISTINCT ts, date, hour, dow, month, year, is_weekend
            FROM tmp_datetime_dim
            ON CONFLICT (ts) DO NOTHING;
        """))
        conn.exec_driver_sql("DROP TABLE tmp_datetime_dim;")

def upsert_carbon_fact(conn, df, region_map):
    if df.empty:
        return
    # Map region text -> region_id
    df = df.copy()
    df["region_id"] = df["region"].map(region_map)
    df["ts"] = pd.to_datetime(df["from_ts"], utc=True)
    df = df[["ts","region_id","forecast","actual","intensity_index"]]
    with conn.begin():
        conn.exec_driver_sql("CREATE TEMP TABLE tmp_carbon_fact (LIKE carbon_fact INCLUDING DEFAULTS) ON COMMIT DROP;")
        df.to_sql("tmp_carbon_fact", conn.connection, if_exists="append", index=False)
        conn.execute(text("""
            INSERT INTO carbon_fact (ts, region_id, forecast, actual, intensity_index, last_updated)
            SELECT ts, region_id, forecast, actual, intensity_index, NOW()
            FROM tmp_carbon_fact
            ON CONFLICT (ts) DO UPDATE
            SET region_id = EXCLUDED.region_id,
                forecast = EXCLUDED.forecast,
                actual = EXCLUDED.actual,
                intensity_index = EXCLUDED.intensity_index,
                last_updated = NOW();
        """))

def main():
    with engine.begin() as conn:
        raw = pd.read_sql("""
            SELECT region, from_ts, to_ts, forecast, actual, intensity_index
            FROM raw_carbon_intensity
            ORDER BY from_ts
        """, con=conn)
        if raw.empty:
            print("No raw data to transform.")
            return

        regions = set(raw["region"].unique())
        region_map = ensure_region_dim(conn, regions)
        ensure_datetime_dim(conn, set(raw["from_ts"].unique()))
        upsert_carbon_fact(conn, raw, region_map)

    print("Transform complete ✅")

if __name__ == "__main__":
    main()
