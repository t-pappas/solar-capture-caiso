"""
analysis_curtailment.py
-----------------------
Computes curtailment-adjusted solar capture price — the metric Modo Energy
uses in their "adjusted capture rate" reports.

Standard capture price only counts MWh that were actually dispatched and
sold at market prices. But curtailed MWh represent real lost revenue:
the solar resource could have generated, but was told not to. The true
economic cost of cannibalization includes both.

Curtailment-adjusted capture price:
    adj_capture = (Σ(lmp × dispatched_mw) + Σ(lmp × curtailed_mw)) / Σ(dispatched_mw + curtailed_mw)
                = Σ(lmp × potential_mw) / Σ(potential_mw)

In practice, curtailment happens almost exclusively during negative and
near-zero price hours — so the adjusted capture price is always *worse*
than the unadjusted, and the gap grows as curtailment increases.

Data sources:
    - data/processed/market_data.csv  (LMP + dispatched solar from pipeline)
    - CAISO Production and Curtailments Data XLS (direct download, no API)
      https://www.caiso.com/documents/production-and-curtailments-data-2025.xlsx

Output:
    outputs/curtailment_analysis.html  (self-contained interactive chart)
    outputs/curtailment_data.csv       (merged dataset for further use)

Run: python src/analysis_curtailment.py
"""

from pathlib import Path
import json

import pandas as pd
import numpy as np

ROOT     = Path(__file__).resolve().parents[1]
PROC_DIR = ROOT / "data" / "processed"
OUT_DIR  = ROOT / "outputs"
OUT_DIR.mkdir(parents=True, exist_ok=True)

PACIFIC = "America/Los_Angeles"

# CAISO publishes annual Production & Curtailments XLS files directly —
# no API key, no ZIP, no rate limits. One file covers a full year.
# ---------------------------------------------------------------------------
# Load & parse curtailment XLS (local files)
# ---------------------------------------------------------------------------
# Download annual files from:
#   https://www.caiso.com/library/production-curtailments-data
# Save to data/raw/ e.g. data/raw/curtailments_2025.xlsx
#
# File structure:
#   Sheet: 'Curtailments'
#   Columns: Date, Hour (1-24, CAISO operating hour), Interval (1-12, 5-min),
#            Wind Curtailment (MW), Solar Curtailment (MW), Reason
#
# The dataset is SPARSE — only intervals with non-zero curtailment are listed.
# Missing intervals = zero curtailment. Hour 1 = 00:00 PST.
# The Date column stores the CAISO trading date with a time portion of 08:00
# (the UTC start of the CAISO trading day) — ignore the time, use date only.

CURTAILMENT_FILES = {
    2023: ROOT / "data" / "raw" / "curtailments_2023.xlsx",
    2024: ROOT / "data" / "raw" / "curtailments_2024.xlsx",
    2025: ROOT / "data" / "raw" / "curtailments_2025.xlsx",
}


def load_curtailment(year: int) -> pd.DataFrame:
    """
    Load CAISO curtailment XLS from data/raw/ and return hourly
    solar curtailment as average MW for each hour (PST).

    Aggregation: the file has 5-minute MW readings (instantaneous rate).
    Since the dataset is sparse, we sum all MW readings in each hour and
    multiply by 5/60 to get MWh curtailed — which is numerically equal
    to the average MW curtailed over the hour. This matches the units of
    solar_mw in market_data.csv (also an hourly average in MW).

    Returns DataFrame with columns: [timestamp, curtailed_mw]
    """
    path = CURTAILMENT_FILES[year]
    if not path.exists():
        raise FileNotFoundError(
            f"Curtailment file not found: {path}\n"
            f"Download from https://www.caiso.com/library/production-curtailments-data\n"
            f"and save as {path.name} in data/raw/"
        )

    print(f"  Loading {path.name}...")
    df = pd.read_excel(path, sheet_name="Curtailments")
    print(f"  Shape: {df.shape} | Columns: {df.columns.tolist()}")

    df["Date"] = pd.to_datetime(df["Date"])
    df["Hour"] = pd.to_numeric(df["Hour"], errors="coerce")
    df["Solar Curtailment"] = pd.to_numeric(df["Solar Curtailment"], errors="coerce").fillna(0).clip(lower=0)
    df = df.dropna(subset=["Date", "Hour"])

    # Build PST timestamp: use date portion only (ignore the 08:00 time in Date),
    # then add (Hour - 1) hours. Hour is 1-indexed CAISO operating hour.
    df["timestamp"] = (
        pd.to_datetime(df["Date"].dt.date.astype(str))
        + pd.to_timedelta(df["Hour"] - 1, unit="h")
    )
    df["timestamp"] = df["timestamp"].dt.tz_localize(
        PACIFIC, ambiguous="NaT", nonexistent="NaT"
    )
    df = df.dropna(subset=["timestamp"])

    # Sum MW readings per hour, multiply by 5/60 to convert to MWh = avg MW
    # (sparse dataset: missing intervals contribute zero)
    hourly = (
        df.groupby("timestamp", as_index=False)["Solar Curtailment"]
        .sum()
        .rename(columns={"Solar Curtailment": "curtailed_mw"})
    )
    hourly["curtailed_mw"] = (hourly["curtailed_mw"] * 5 / 60).round(2)

    n_nonzero = (hourly["curtailed_mw"] > 0).sum()
    total_gwh = hourly["curtailed_mw"].sum() / 1000
    print(f"  {len(hourly)} hourly rows | {n_nonzero} hours with curtailment | {total_gwh:.1f} GWh total")

    return hourly.sort_values("timestamp").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Merge with market data and compute metrics
# ---------------------------------------------------------------------------

def compute_adjusted_capture(market: pd.DataFrame, curtailment: pd.DataFrame) -> pd.DataFrame:
    """
    Merge dispatched generation + LMPs with curtailment data.
    Computes standard and curtailment-adjusted capture metrics per day.
    """
    df = pd.merge(market, curtailment, on="timestamp", how="left")
    df["curtailed_mw"] = df["curtailed_mw"].fillna(0)
    df["potential_mw"] = df["solar_mw"] + df["curtailed_mw"]

    df["date"] = df["timestamp"].dt.date

    daily = df.groupby("date").apply(lambda g: pd.Series({
        # Standard capture (dispatched MWh only)
        "capture_price":    (g["sp15_lmp"] * g["solar_mw"]).sum() / max(g["solar_mw"].sum(), 1),
        "avg_lmp":          g["sp15_lmp"].mean(),
        "solar_gwh":        g["solar_mw"].sum() / 1000,
        # Curtailment-adjusted capture (dispatched + curtailed potential)
        "adj_capture_price": (g["sp15_lmp"] * g["potential_mw"]).sum() / max(g["potential_mw"].sum(), 1),
        "curtailed_gwh":    g["curtailed_mw"].sum() / 1000,
        "potential_gwh":    g["potential_mw"].sum() / 1000,
        # Curtailment rate
        "curtailment_rate": g["curtailed_mw"].sum() / max(g["potential_mw"].sum(), 1),
    })).reset_index()

    daily["date"] = pd.to_datetime(daily["date"])
    daily["capture_ratio"]     = daily["capture_price"]     / daily["avg_lmp"]
    daily["adj_capture_ratio"] = daily["adj_capture_price"] / daily["avg_lmp"]
    daily["adj_discount"]      = daily["capture_ratio"] - daily["adj_capture_ratio"]

    return daily.round(3)


# ---------------------------------------------------------------------------
# HTML dashboard
# ---------------------------------------------------------------------------

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Curtailment-adjusted capture — CAISO Solar</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0b0b0b;color:#e0ddd6;padding:28px 22px}
.page{max-width:960px;margin:0 auto}
h1{font-size:20px;font-weight:500;color:#f0ede8;margin-bottom:4px}
.meta{font-size:13px;color:#9a9890;margin-bottom:20px}
.explainer{background:#111;border:1px solid #1e1e1e;border-radius:8px;padding:14px 18px;margin-bottom:20px;font-size:14px;color:#c8c5be;line-height:1.65}
.explainer strong{color:#d3d1c7;font-weight:500}
.kpi-row{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin-bottom:20px}
.kpi{background:#111;border:1px solid #1e1e1e;border-radius:6px;padding:12px 14px}
.kpi-lbl{font-size:13px;letter-spacing:0.9px;color:#9a9890;text-transform:uppercase;margin-bottom:7px}
.kpi-val{font-size:22px;font-weight:500;color:#f0ede8;line-height:1}
.kpi-sub{font-size:13px;margin-top:5px;color:#b0aea8}
.red{color:#e24b4a}.amber{color:#ef9f27}.green{color:#639922}
.chart-block{background:#111;border:1px solid #1e1e1e;border-radius:8px;padding:16px 18px;margin-bottom:16px}
.fig-eye{font-size:13px;letter-spacing:1.4px;color:#8a8880;text-transform:uppercase;margin-bottom:4px}
.fig-title{font-size:13px;font-weight:500;color:#c8c5be;margin-bottom:3px}
.fig-sub{font-size:13px;color:#9a9890;margin-bottom:12px;line-height:1.5}
.leg-row{display:flex;gap:14px;margin-bottom:10px;flex-wrap:wrap}
.leg{display:flex;align-items:center;gap:5px;font-size:13px;color:#b0aea8}
.leg-line{width:18px;height:2px;border-radius:1px}
.leg-dash{width:18px;height:0;border-top:2px dashed}
.callout{background:#1a1200;border-left:2px solid #854f0b;border-radius:0 4px 4px 0;padding:8px 12px;margin-top:10px;font-size:13px;color:#b0aea8;line-height:1.6}
.callout strong{color:#ef9f27;font-weight:500}
</style>
</head>
<body><div class="page">
<h1>Curtailment-adjusted solar capture — CAISO SP15</h1>
<div class="meta" id="meta"></div>
<div class="explainer">
  <strong>What this measures:</strong> Standard capture price only counts dispatched MWh — energy that was generated and sold. But curtailed solar represents real lost revenue: the asset could have generated, but was instructed not to (usually because there was too much supply and prices were already negative). The <strong>curtailment-adjusted capture rate</strong> adds curtailed potential back in, showing the true economic impact of cannibalization. A widening gap between the two lines means curtailment is becoming a larger share of the value problem.
</div>
<div class="kpi-row" id="kpis"></div>
<div class="chart-block">
  <div class="fig-eye">Figure 1</div>
  <div class="fig-title">Standard vs curtailment-adjusted capture price — daily</div>
  <div class="fig-sub">The gap between lines is the daily curtailment cost. When it widens, curtailment events are coinciding with non-trivial prices.</div>
  <div class="leg-row">
    <span class="leg"><span class="leg-line" style="background:#378add"></span>Avg SP15 LMP</span>
    <span class="leg"><span class="leg-line" style="background:#ef9f27"></span>Standard capture price</span>
    <span class="leg"><span class="leg-dash" style="border-color:#e24b4a"></span>Curtailment-adjusted capture</span>
  </div>
  <div style="position:relative;height:220px"><canvas id="c1"></canvas></div>
  <div class="callout" id="c1-note"></div>
</div>
<div class="chart-block">
  <div class="fig-eye">Figure 2</div>
  <div class="fig-title">Daily curtailment volume and rate</div>
  <div class="fig-sub">Bars show curtailed GWh per day. Line shows curtailment as % of total potential generation. Spikes mark high-penetration events where supply overwhelmed demand.</div>
  <div class="leg-row">
    <span class="leg"><span class="leg-line" style="background:#711;opacity:0.6"></span>Curtailed GWh</span>
    <span class="leg"><span class="leg-line" style="background:#e24b4a"></span>Curtailment rate (%)</span>
  </div>
  <div style="position:relative;height:180px"><canvas id="c2"></canvas></div>
  <div class="callout" id="c2-note"></div>
</div>
</div>
<script>
const D = __DATA__;
const GRID='#1a1a1a', TICK={color:'#8a8880',font:{size:12}};

function fmt$(v){return (v<0?'-$'+Math.abs(v).toFixed(2):'$'+v.toFixed(2));}

const kd = D.kpis;
document.getElementById('meta').textContent = D.period + ' · SP15 hub · curtailment-adjusted capture rate';
document.getElementById('kpis').innerHTML = [
  {l:'Standard capture', v:fmt$(kd.capture)+'/MWh', s:'Dispatched MWh only', c:'amber'},
  {l:'Adjusted capture', v:fmt$(kd.adj_capture)+'/MWh', s:'Incl. curtailed potential', c:'red'},
  {l:'Curtailment cost', v:'$'+(kd.capture-kd.adj_capture).toFixed(2)+'/MWh', s:'Additional discount from curtailment', c:'red'},
  {l:'Curtailment rate', v:(kd.curtailment_rate*100).toFixed(1)+'%', s:'Share of potential gen curtailed', c: kd.curtailment_rate>0.1?'red':'amber'},
].map(k=>`<div class="kpi"><div class="kpi-lbl">${k.l}</div><div class="kpi-val">${k.v}</div><div class="kpi-sub ${k.c}">${k.s}</div></div>`).join('');

const labels = D.daily.date.map(d=>d.slice(5));
new Chart(document.getElementById('c1'),{
  data:{labels, datasets:[
    {type:'line',data:D.daily.avg_lmp,borderColor:'#378add',borderWidth:1.5,pointRadius:0,tension:0.3,yAxisID:'y'},
    {type:'line',data:D.daily.capture,borderColor:'#ef9f27',borderWidth:1.8,pointRadius:0,tension:0.3,yAxisID:'y'},
    {type:'line',data:D.daily.adj_capture,borderColor:'#e24b4a',borderWidth:1.5,borderDash:[4,3],pointRadius:0,tension:0.3,yAxisID:'y'},
  ]},
  options:{responsive:true,maintainAspectRatio:false,
    plugins:{legend:{display:false},tooltip:{backgroundColor:'#161616',borderColor:'#222',borderWidth:1,titleColor:'#c8c5be',bodyColor:'#666'}},
    scales:{x:{ticks:{...TICK,maxTicksLimit:10,maxRotation:0},grid:{color:GRID}},
            y:{ticks:{...TICK,callback:v=>v<0?`-$${Math.abs(v)}`:`$${v}`},grid:{color:GRID}}}}
});

new Chart(document.getElementById('c2'),{
  data:{labels, datasets:[
    {type:'bar', data:D.daily.curtailed_gwh,backgroundColor:'rgba(180,50,50,0.4)',borderColor:'#711',borderWidth:0.5,yAxisID:'y',borderRadius:1},
    {type:'line',data:D.daily.curtailment_rate.map(v=>+(v*100).toFixed(1)),borderColor:'#e24b4a',borderWidth:1.8,pointRadius:0,tension:0.3,yAxisID:'y2'},
  ]},
  options:{responsive:true,maintainAspectRatio:false,
    plugins:{legend:{display:false},tooltip:{backgroundColor:'#161616',borderColor:'#222',borderWidth:1,titleColor:'#c8c5be',bodyColor:'#666'}},
    scales:{x:{ticks:{...TICK,maxTicksLimit:10,maxRotation:0},grid:{color:GRID}},
            y:{position:'left',ticks:{...TICK,callback:v=>`${v}GWh`},grid:{color:GRID}},
            y2:{position:'right',ticks:{...TICK,callback:v=>`${v}%`},grid:{display:false}}}}
});

const maxCurtDay = D.daily.date[D.daily.curtailed_gwh.indexOf(Math.max(...D.daily.curtailed_gwh))].slice(5);
const maxCurt = Math.max(...D.daily.curtailed_gwh).toFixed(1);
document.getElementById('c1-note').innerHTML =
  `Adjusted capture is consistently below standard capture. The dashed line dips furthest on high-curtailment days — when negative-price hours are also the hours with the most curtailment, the true economic loss compounds.`;
document.getElementById('c2-note').innerHTML =
  `Peak curtailment day: <strong>${maxCurtDay} (${maxCurt} GWh curtailed)</strong>. High-curtailment days almost always coincide with the lowest standard capture prices — confirming that curtailment and price suppression are driven by the same oversupply events.`;
</script></body></html>"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 55)
    print("analysis_curtailment.py — curtailment-adjusted capture")
    print("=" * 55)

    # Load market data
    market = pd.read_csv(PROC_DIR / "market_data.csv")
    market["timestamp"] = pd.to_datetime(market["timestamp"])
    if market["timestamp"].dt.tz is None:
        market["timestamp"] = market["timestamp"].dt.tz_localize(PACIFIC)

    months = market["timestamp"].dt.to_period("M").unique()
    years  = sorted({p.year for p in months})
    print(f"\nMarket data covers: {[str(m) for m in months]}")
    print(f"Years needed: {years}")

    # Load curtailment data from local files
    curtailment_frames = []
    for year in years:
        if year not in CURTAILMENT_FILES:
            print(f"  No curtailment file configured for {year}, skipping")
            continue
        try:
            hourly = load_curtailment(year)
            curtailment_frames.append(hourly)
        except FileNotFoundError as e:
            print(f"  WARNING: {e}")

    if not curtailment_frames:
        raise RuntimeError(
            "No curtailment data loaded.\n"
            "Download files from https://www.caiso.com/library/production-curtailments-data\n"
            "and save to data/raw/curtailments_YYYY.xlsx"
        )

    curtailment = pd.concat(curtailment_frames).sort_values("timestamp").reset_index(drop=True)

    # Align timezones
    mkt_tz = market["timestamp"].dt.tz
    if curtailment["timestamp"].dt.tz != mkt_tz:
        curtailment["timestamp"] = curtailment["timestamp"].dt.tz_convert(mkt_tz)

    # Save hourly curtailment to data/processed/ so it sits alongside market_data.csv
    # and can be loaded by other scripts without re-parsing the raw XLS each time.
    # Columns: timestamp (PST), curtailed_mw (avg MW curtailed that hour)
    hourly_path = PROC_DIR / "curtailment_hourly.csv"
    curtailment.to_csv(hourly_path, index=False)
    print(f"Saved hourly curtailment → {hourly_path}")

    # Also save the full merged hourly dataset (market + curtailment) for convenience
    merged = pd.merge(market, curtailment, on="timestamp", how="left")
    merged["curtailed_mw"]  = merged["curtailed_mw"].fillna(0)
    merged["potential_mw"]  = merged["solar_mw"] + merged["curtailed_mw"]
    merged_path = PROC_DIR / "market_data_with_curtailment.csv"
    merged.to_csv(merged_path, index=False)
    print(f"Saved merged hourly data  → {merged_path}")

    # Compute metrics
    print("\nComputing curtailment-adjusted capture metrics...")
    daily = compute_adjusted_capture(market, curtailment)

    # Save CSV
    csv_path = OUT_DIR / "curtailment_data.csv"
    daily.to_csv(csv_path, index=False)
    print(f"Saved CSV → {csv_path}")

    # Summary KPIs
    total_solar    = daily["solar_gwh"].sum()
    total_curtail  = daily["curtailed_gwh"].sum()
    total_potential = total_solar + total_curtail
    cap_price      = (daily["capture_price"]     * daily["solar_gwh"]).sum()     / total_solar
    adj_cap_price  = (daily["adj_capture_price"] * daily["potential_gwh"]).sum() / total_potential
    avg_lmp        = daily["avg_lmp"].mean()
    curtail_rate   = total_curtail / total_potential

    print(f"\nSummary:")
    print(f"  Standard capture price    : ${cap_price:.2f}/MWh")
    print(f"  Adjusted capture price    : ${adj_cap_price:.2f}/MWh")
    print(f"  Curtailment cost          : ${cap_price - adj_cap_price:.2f}/MWh")
    print(f"  Overall curtailment rate  : {curtail_rate:.1%}")
    print(f"  Total solar dispatched    : {total_solar:.0f} GWh")
    print(f"  Total curtailed           : {total_curtail:.0f} GWh")

    # Build HTML payload
    payload = {
        "period": f"{daily['date'].min().strftime('%b %Y')} – {daily['date'].max().strftime('%b %Y')}",
        "kpis": {
            "capture":          round(cap_price, 2),
            "adj_capture":      round(adj_cap_price, 2),
            "curtailment_rate": round(curtail_rate, 4),
            "avg_lmp":          round(avg_lmp, 2),
        },
        "daily": {
            "date":             [str(d) for d in daily["date"]],
            "avg_lmp":          daily["avg_lmp"].round(2).tolist(),
            "capture":          daily["capture_price"].round(2).tolist(),
            "adj_capture":      daily["adj_capture_price"].round(2).tolist(),
            "curtailed_gwh":    daily["curtailed_gwh"].round(2).tolist(),
            "curtailment_rate": daily["curtailment_rate"].round(4).tolist(),
        }
    }

    html = HTML.replace("__DATA__", json.dumps(payload))
    out  = OUT_DIR / "curtailment_analysis.html"
    out.write_text(html, encoding="utf-8")
    print(f"Dashboard saved → {out}")


if __name__ == "__main__":
    main()
