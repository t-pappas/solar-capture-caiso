"""
download_data.py
----------------
Downloads three datasets from the CAISO OASIS API:
  - NP15 hub LMP (PRC_LMP, DAM)
  - SP15 hub LMP (PRC_LMP, DAM)
  - System solar generation (SLD_REN_FCST, ACTUAL)

Outputs to data/raw/:
  lmp_np15.csv, lmp_sp15.csv, solar_generation.csv

Notes on OASIS quirks:
  - Use http:// not https:// (self-signed cert on macOS/Anaconda will fail)
  - version=1 is required or you get INVALID_REQUEST
  - resultformat=6 returns CSV inside a ZIP
  - Request one month at a time to avoid 429 rate limits
  - Solar: filter RENEWABLE_TYPE == "Solar" and MARKET_RUN_ID == "ACTUAL"
  - LMP:   filter NODE_ID (not NODE) and LMP_TYPE == "LMP"
"""

from pathlib import Path
import requests
import zipfile
import io
import time
import pandas as pd
from datetime import datetime

# http (not https) avoids SSL cert errors on macOS with Anaconda Python
BASE_URL = "http://oasis.caiso.com/oasisapi/SingleZip"

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

NP15 = "TH_NP15_GEN-APND"
SP15 = "TH_SP15_GEN-APND"


# ---------------------------------------------------------------------------
# Core download helper
# ---------------------------------------------------------------------------

def download_oasis(params: dict, retries: int = 3, backoff: float = 10.0) -> pd.DataFrame:
    """
    Hit the OASIS SingleZip endpoint, unzip, return a DataFrame.
    Retries on 429 (rate limit) with exponential backoff.
    """
    params = {**params, "version": 1, "resultformat": 6}

    for attempt in range(1, retries + 1):
        try:
            r = requests.get(BASE_URL, params=params, timeout=120)
        except requests.exceptions.ConnectionError as e:
            raise RuntimeError(
                f"Connection failed. Make sure BASE_URL uses http:// not https://\n{e}"
            )

        if r.status_code == 429:
            wait = backoff * attempt
            print(f"  Rate limited (429). Waiting {wait:.0f}s before retry {attempt}/{retries}...")
            time.sleep(wait)
            continue

        r.raise_for_status()

        # OASIS returns a ZIP containing either CSV or XML
        try:
            z = zipfile.ZipFile(io.BytesIO(r.content))
        except zipfile.BadZipFile:
            raise RuntimeError(f"Response is not a ZIP. Raw content:\n{r.text[:500]}")

        fname = z.namelist()[0]
        content = z.read(fname)

        if fname.endswith(".xml"):
            # XML means an API error message
            raise RuntimeError(f"OASIS returned XML (likely an error):\n{content.decode()[:1000]}")

        df = pd.read_csv(io.BytesIO(content))
        return df

    raise RuntimeError("Max retries exceeded (rate limited).")


# ---------------------------------------------------------------------------
# Dataset-specific extractors
# ---------------------------------------------------------------------------

def get_lmp(start: datetime, end: datetime, node: str) -> pd.DataFrame:
    """
    Download hourly Day-Ahead Market LMP for a single hub node.
    Returns: DataFrame with columns [timestamp, lmp]
    """
    params = {
        "queryname": "PRC_LMP",
        "startdatetime": start.strftime("%Y%m%dT08:00-0000"),
        "enddatetime":   end.strftime("%Y%m%dT08:00-0000"),
        "market_run_id": "DAM",
        "grp_type":      "ALL",
    }
    print(f"  Fetching PRC_LMP for {node} [{start.date()} → {end.date()}]")
    df = download_oasis(params)

    # Filter to energy LMP only (exclude loss/congestion components)
    df = df[df["LMP_TYPE"] == "LMP"]

    # IMPORTANT: column is NODE_ID, not NODE
    df = df[df["NODE_ID"] == node]

    if df.empty:
        raise ValueError(
            f"No LMP rows found for node '{node}'. "
            f"Available NODE_IDs: {df['NODE_ID'].unique()[:10].tolist()}"
        )

    df = df.rename(columns={"INTERVALSTARTTIME_GMT": "timestamp", "MW": "lmp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    return df[["timestamp", "lmp"]].sort_values("timestamp").reset_index(drop=True)


def get_solar(start: datetime, end: datetime) -> pd.DataFrame:
    """
    Download hourly actual solar generation from SLD_REN_FCST.
    Returns: DataFrame with columns [timestamp, solar_mw]

    SLD_REN_FCST gives system-wide renewable generation by type.
    Filtering RENEWABLE_TYPE==Solar + MARKET_RUN_ID==ACTUAL gives
    the realized hourly solar output across CAISO.
    """
    params = {
        "queryname":     "SLD_REN_FCST",
        "startdatetime": start.strftime("%Y%m%dT08:00-0000"),
        "enddatetime":   end.strftime("%Y%m%dT08:00-0000"),
        "market_run_id": "ACTUAL",
    }
    print(f"  Fetching SLD_REN_FCST (Solar ACTUAL) [{start.date()} → {end.date()}]")
    df = download_oasis(params)

    df = df[df["RENEWABLE_TYPE"] == "Solar"]
    df = df[df["MARKET_RUN_ID"] == "ACTUAL"]

    if df.empty:
        raise ValueError("No solar rows found. Check RENEWABLE_TYPE and MARKET_RUN_ID columns.")

    # Build timestamp from OPR_DT + OPR_HR (OPR_HR is 1-indexed)
    df["timestamp"] = (
        pd.to_datetime(df["OPR_DT"])
        + pd.to_timedelta(df["OPR_HR"] - 1, unit="h")
    )
    # Treat as Pacific time and convert to UTC to align with LMP timestamps
    df["timestamp"] = df["timestamp"].dt.tz_localize("America/Los_Angeles", ambiguous="NaT", nonexistent="NaT")
    df["timestamp"] = df["timestamp"].dt.tz_convert("UTC")

    df = df.rename(columns={"MW": "solar_mw"})
    return df[["timestamp", "solar_mw"]].sort_values("timestamp").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # One month of data — enough to demonstrate the methodology clearly.
    # Requesting one month at a time avoids 429 rate limits.
    start = datetime(2025, 1, 1)
    end   = datetime(2025, 2, 1)

    print(f"\nDownloading CAISO market data: {start.date()} to {end.date()}")
    print("=" * 50)

    print("\n[1/3] NP15 LMP (Northern California hub)")
    lmp_np15 = get_lmp(start, end, NP15)
    lmp_np15.to_csv(RAW_DIR / "lmp_np15.csv", index=False)
    print(f"  Saved {len(lmp_np15)} rows → data/raw/lmp_np15.csv")

    # Small pause between requests to be polite to the API
    time.sleep(3)

    print("\n[2/3] SP15 LMP (Southern California hub)")
    lmp_sp15 = get_lmp(start, end, SP15)
    lmp_sp15.to_csv(RAW_DIR / "lmp_sp15.csv", index=False)
    print(f"  Saved {len(lmp_sp15)} rows → data/raw/lmp_sp15.csv")

    time.sleep(3)

    print("\n[3/3] Solar generation (system-wide actual)")
    solar = get_solar(start, end)
    solar.to_csv(RAW_DIR / "solar_generation.csv", index=False)
    print(f"  Saved {len(solar)} rows → data/raw/solar_generation.csv")

    print("\nDone. Expected ~744 rows per file (31 days × 24 hours).")
    print(f"Output directory: {RAW_DIR}")


if __name__ == "__main__":
    main()
