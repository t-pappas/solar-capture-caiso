"""
process_data.py
---------------
Loads the three raw CSVs, aligns them on UTC timestamp, then converts
all timestamps to America/Los_Angeles (PST/PDT) before saving.

Timestamps are stored in local time so that hour-of-day groupings in
analysis scripts (duck curve, heatmap, capture price) are correct for
California markets. Solar peaks at PST 08–15, not UTC 16–23.

    data/processed/market_data.csv

Columns:
    timestamp         — Pacific time (PST/PDT), hourly
    np15_lmp          — NP15 hub Day-Ahead LMP ($/MWh)
    sp15_lmp          — SP15 hub Day-Ahead LMP ($/MWh)
    solar_mw          — CAISO system solar generation (MW)
    hub_avg_lmp       — simple average of NP15 and SP15 ($/MWh)

The merge is an inner join on timestamp — only hours present in all three
datasets are kept. For a full month expect ~700-744 rows.
"""

from pathlib import Path
import pandas as pd

ROOT     = Path(__file__).resolve().parents[1]
RAW_DIR  = ROOT / "data" / "raw"
PROC_DIR = ROOT / "data" / "processed"
PROC_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = PROC_DIR / "market_data.csv"

PACIFIC  = "America/Los_Angeles"


def load_raw() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load raw CSVs and parse timestamps as UTC."""

    def _load(fname):
        df = pd.read_csv(RAW_DIR / fname)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df

    np15  = _load("lmp_np15.csv")
    sp15  = _load("lmp_sp15.csv")
    solar = _load("solar_generation.csv")

    return np15, sp15, solar


def validate(df: pd.DataFrame, name: str, value_col: str) -> pd.DataFrame:
    nulls = df[value_col].isna().sum()
    dupes = df["timestamp"].duplicated().sum()
    print(f"  {name}: {len(df)} rows | {nulls} nulls | {dupes} duplicate timestamps")
    if dupes > 0:
        print(f"    Warning: dropping {dupes} duplicate timestamps in {name}")
        df = df.drop_duplicates(subset="timestamp", keep="first")
    return df


def process() -> pd.DataFrame:
    print("Loading raw data...")
    np15, sp15, solar = load_raw()

    print("\nValidating inputs:")
    np15  = validate(np15,  "lmp_np15",         "lmp")
    sp15  = validate(sp15,  "lmp_sp15",         "lmp")
    solar = validate(solar, "solar_generation", "solar_mw")

    # Rename before merging so columns don't collide
    np15 = np15.rename(columns={"lmp": "np15_lmp"})
    sp15 = sp15.rename(columns={"lmp": "sp15_lmp"})

    # Inner join on UTC timestamp — only keep hours present in all three datasets
    print("\nMerging on timestamp (inner join)...")
    df = pd.merge(np15, sp15,  on="timestamp", how="inner")
    df = pd.merge(df,   solar, on="timestamp", how="inner")

    # Convert UTC → Pacific time.
    # This is the single source of truth for timezone handling.
    # All downstream scripts (analysis.py, analysis_dashboard.py) read this
    # file and get correct PST/PDT hours automatically — no per-script conversion needed.
    print(f"\nConverting timestamps: UTC → {PACIFIC}")
    df["timestamp"] = df["timestamp"].dt.tz_convert(PACIFIC)
    sample_utc_offset = df["timestamp"].iloc[0].utcoffset()
    print(f"  UTC offset for first row: {sample_utc_offset} "
          f"({'PST (UTC-8)' if str(sample_utc_offset) == '-1 day, 16:00:00' else 'PDT (UTC-7)'})")

    # Derived column: simple hub average
    df["hub_avg_lmp"] = (df["np15_lmp"] + df["sp15_lmp"]) / 2

    df = df.sort_values("timestamp").reset_index(drop=True)

    # Sanity check: solar should peak in PST hours 08–15
    df["_hour"] = df["timestamp"].dt.hour
    peak_hour = df.groupby("_hour")["solar_mw"].mean().idxmax()
    print(f"  Solar peak hour (PST): {peak_hour:02d}:00 — "
          f"{'OK (should be 08–15)' if 8 <= peak_hour <= 15 else 'WARNING: unexpected'}")
    df = df.drop(columns=["_hour"])

    print(f"\nMerged dataset: {len(df)} hourly rows")
    print(f"  Date range : {df['timestamp'].min()} → {df['timestamp'].max()}")
    print(f"  SP15 LMP   : ${df['sp15_lmp'].mean():.2f}/MWh avg  "
          f"(min ${df['sp15_lmp'].min():.1f}, max ${df['sp15_lmp'].max():.1f})")
    print(f"  Solar      : {df['solar_mw'].mean():.0f} MW avg  "
          f"(peak {df['solar_mw'].max():.0f} MW)")

    return df


def main():
    print("=" * 50)
    print("process_data.py — merging raw CAISO datasets")
    print("=" * 50)

    df = process()

    df.to_csv(OUT_FILE, index=False)
    print(f"\nSaved → {OUT_FILE}")
    print("\nPreview:")
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
