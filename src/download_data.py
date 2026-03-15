"""
download_data.py
----------------
Downloads three datasets from the CAISO OASIS API:
  - NP15 hub LMP (PRC_LMP, DAM)
  - SP15 hub LMP (PRC_LMP, DAM)
  - System solar generation (SLD_REN_FCST, ACTUAL)

Outputs to data/raw/:
  lmp_np15.csv, lmp_sp15.csv, solar_generation.csv

OASIS quirks handled here:
  - verify=False: CAISO uses a self-signed cert that fails on macOS/Anaconda.
    http:// doesn't help — the server redirects to https:// anyway.
  - version=1 is required or you get INVALID_REQUEST
  - resultformat=6 returns CSV inside a ZIP
  - PRC_LMP returns max 24 hours per request — must loop daily
  - SLD_REN_FCST returns 5 MARKET_RUN_IDs (ACTUAL/DAM/HASP/RTD/RTPD)
    and 3 TRADING_HUBs (NP15/SP15/SCEZ) per hour — filter + sum correctly
  - Solar negatives are curtailment accounting artifacts — clamp to zero
"""

from pathlib import Path
import requests
import urllib3
import zipfile
import io
import time
import pandas as pd
from datetime import datetime

# CAISO uses a self-signed certificate that fails on macOS/Anaconda.
# verify=False disables cert checking. We suppress the urllib3 warning
# since this is a known, intentional workaround for a specific trusted host.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://oasis.caiso.com/oasisapi/SingleZip"

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
            r = requests.get(BASE_URL, params=params, timeout=120, verify=False)
        except requests.exceptions.ConnectionError as e:
            raise RuntimeError(f"Connection failed:\n{e}")

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

    Key insight: pass node= directly as an API param so CAISO filters
    server-side. This returns ~120 rows (24hrs × 5 LMP types) instead of
    the full grid (~100k rows), making each request ~50x smaller and faster.
    With a specific node we can also request a full month in one call.

    Returns: DataFrame with columns [timestamp, lmp]
    """
    params = {
        "queryname":     "PRC_LMP",
        "startdatetime": start.strftime("%Y%m%dT08:00-0000"),
        "enddatetime":   end.strftime("%Y%m%dT08:00-0000"),
        "market_run_id": "DAM",
        "node":          node,       # server-side filter — no grp_type needed
    }
    print(f"  Fetching PRC_LMP for {node} [{start.date()} → {end.date()}]")
    df = download_oasis(params)

    # Keep energy component only (excludes congestion MCC, loss MCE, etc.)
    df = df[df["LMP_TYPE"] == "LMP"]

    if df.empty:
        raise ValueError(
            f"No LMP rows found for node '{node}'. "
            f"Available NODE_IDs: {df['NODE_ID'].unique()[:10].tolist()}"
        )

    df = df.rename(columns={"INTERVALSTARTTIME_GMT": "timestamp", "MW": "lmp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.drop_duplicates(subset="timestamp")

    return df[["timestamp", "lmp"]].sort_values("timestamp").reset_index(drop=True)


def get_solar(start: datetime, end: datetime) -> pd.DataFrame:
    """
    Download hourly actual solar generation from SLD_REN_FCST.
    Returns: DataFrame with columns [timestamp, solar_mw]

    The dataset has a TRADING_HUB column (NP15 / SP15 / SCEZ) — we sum
    across all hubs to get total CAISO system solar generation.
    Five MARKET_RUN_ID values exist: ACTUAL, DAM, HASP, RTD, RTPD.
    We keep only ACTUAL. Negatives are curtailment accounting artifacts
    and are clamped to zero.
    """
    params = {
        "queryname":     "SLD_REN_FCST",
        "startdatetime": start.strftime("%Y%m%dT08:00-0000"),
        "enddatetime":   end.strftime("%Y%m%dT08:00-0000"),
        "market_run_id": "ACTUAL",
    }
    print(f"  Fetching SLD_REN_FCST (Solar ACTUAL) [{start.date()} → {end.date()}]")
    df = download_oasis(params)

    # Keep solar actuals only
    df = df[df["RENEWABLE_TYPE"] == "Solar"]
    df = df[df["MARKET_RUN_ID"] == "ACTUAL"]

    if df.empty:
        raise ValueError("No solar rows found after filtering.")

    # Use INTERVALSTARTTIME_GMT directly — already UTC, no timezone conversion needed
    df = df.rename(columns={"INTERVALSTARTTIME_GMT": "timestamp", "MW": "solar_mw"})
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    # Sum across trading hubs (NP15 + SP15 + SCEZ) to get system-wide total
    df = df.groupby("timestamp", as_index=False)["solar_mw"].sum()

    # Clamp negatives to zero — these are curtailment accounting entries,
    # not real generation. A solar fleet cannot produce negative MWh.
    df["solar_mw"] = df["solar_mw"].clip(lower=0)

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
