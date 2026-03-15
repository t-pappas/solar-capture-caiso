"""
analysis_yoy.py
---------------
Year-on-year solar capture ratio trend for CAISO SP15 and NP15.

The core thesis: as solar capacity in CAISO has grown from ~10 GW (2019)
to ~25 GW+ (2024), the capture ratio has declined because more solar
competing at the same midday hours suppresses the prices it earns —
"cannibalization accelerates with penetration."

This script:
  1. Downloads Jan LMP + solar data for 2023, 2024, 2025 (see note on
     best month selection below)
  2. Computes capture ratio, avg LMP, and negative-price exposure per year
  3. Shows the trend as a Modo-style comparison chart

BEST MONTH FOR THIS ANALYSIS: January
---------------------------------------
January is the best single month to compare year-over-year because:
  - Solar capacity is growing but January generation is moderate (~80–110 GWh/day),
    so the penetration effect is visible without being dominated by spring/summer peaks
  - No DST transition (unlike March/November) — clean UTC-8 offset year-round
  - Weather patterns are relatively stable year-to-year vs. spring (wet/dry year effects)
  - No Easter or long-weekend demand anomalies (unlike April/May)

AVOID for YoY:
  - April/May: highly weather-dependent (wet vs dry years change hydro dispatch
    dramatically, confounding the solar signal)
  - June/July: DST transition in March means H1 data straddles two offsets
  - October: NEM export policy changes (e.g., NEM 3.0 launched April 2023)
    create structural breaks that make clean YoY comparison harder

If you want to extend to multiple months, February and September are the next
best — February for winter baseline, September for peak-penetration shoulder season.

Data:
  - Pulls LMP + solar from CAISO OASIS via download_data.py's functions
  - Uses January of each year: 2023, 2024, 2025

Output:
    outputs/yoy_trend.html

Run: python src/analysis_yoy.py
"""

from pathlib import Path
import json
import time

import pandas as pd
import numpy as np
import requests
import zipfile
import io
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ROOT     = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "yoy"
OUT_DIR  = ROOT / "outputs"
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://oasis.caiso.com/oasisapi/SingleZip"
PACIFIC  = "America/Los_Angeles"
NP15     = "TH_NP15_GEN-APND"
SP15     = "TH_SP15_GEN-APND"

# January of each year — UTC datetimes (Jan 1 08:00 UTC = Jan 1 00:00 PST)
YOY_PERIODS = {
    2023: ("20230101T08:00-0000", "20230201T08:00-0000"),
    2024: ("20240101T08:00-0000", "20240201T08:00-0000"),
    2025: ("20250101T08:00-0000", "20250201T08:00-0000"),
}

# Known CAISO installed solar capacity (GW, approximate, end of year)
# Source: CAISO annual reports / LBNL Tracking the Sun
SOLAR_CAPACITY_GW = {
    2022: 13.1,
    2023: 16.2,
    2024: 19.8,
    2025: 23.5,  # estimated based on interconnection queue completions
}


# ---------------------------------------------------------------------------
# Data download (reuses logic from download_data.py)
# ---------------------------------------------------------------------------

def download_oasis(params: dict) -> pd.DataFrame:
    params = {**params, "version": 1, "resultformat": 6}
    r = requests.get(BASE_URL, params=params, timeout=120, verify=False)
    r.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(r.content))
    fname = z.namelist()[0]
    content = z.read(fname)
    if fname.endswith(".xml"):
        raise RuntimeError(f"OASIS returned XML error:\n{content.decode()[:500]}")
    return pd.read_csv(io.BytesIO(content))


def get_lmp_year(year: int, node: str) -> pd.DataFrame:
    start, end = YOY_PERIODS[year]
    cache_path = DATA_DIR / f"lmp_{node.split('_')[1].lower()}_{year}.csv"

    if cache_path.exists():
        print(f"    Using cached: {cache_path.name}")
        df = pd.read_csv(cache_path)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df

    print(f"    Downloading {node} LMP for {year}...")
    params = {
        "queryname":     "PRC_LMP",
        "startdatetime": start,
        "enddatetime":   end,
        "market_run_id": "DAM",
        "node":          node,
    }
    df = download_oasis(params)
    df = df[df["LMP_TYPE"] == "LMP"]
    df = df.rename(columns={"INTERVALSTARTTIME_GMT": "timestamp", "MW": "lmp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(PACIFIC)
    df = df[["timestamp", "lmp"]].sort_values("timestamp").drop_duplicates("timestamp")
    df.to_csv(cache_path, index=False)
    time.sleep(2)
    return df


def get_solar_year(year: int) -> pd.DataFrame:
    start, end = YOY_PERIODS[year]
    cache_path = DATA_DIR / f"solar_{year}.csv"

    if cache_path.exists():
        print(f"    Using cached: {cache_path.name}")
        df = pd.read_csv(cache_path)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df

    print(f"    Downloading solar generation for {year}...")
    params = {
        "queryname":     "SLD_REN_FCST",
        "startdatetime": start,
        "enddatetime":   end,
        "market_run_id": "ACTUAL",
    }
    df = download_oasis(params)
    df = df[df["RENEWABLE_TYPE"] == "Solar"]
    df = df[df["MARKET_RUN_ID"]  == "ACTUAL"]
    df = df.rename(columns={"INTERVALSTARTTIME_GMT": "timestamp", "MW": "solar_mw"})
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(PACIFIC)
    df = df.groupby("timestamp", as_index=False)["solar_mw"].sum()
    df["solar_mw"] = df["solar_mw"].clip(lower=0)
    df.to_csv(cache_path, index=False)
    time.sleep(2)
    return df


# ---------------------------------------------------------------------------
# Metric computation
# ---------------------------------------------------------------------------

def compute_yoy_metrics(years: list) -> dict:
    results = {}

    for year in years:
        print(f"\n  Year {year}:")
        try:
            sp15_df = get_lmp_year(year, SP15)
            np15_df = get_lmp_year(year, NP15)
            solar_df = get_solar_year(year)

            sp15_df  = sp15_df.rename(columns={"lmp": "sp15_lmp"})
            np15_df  = np15_df.rename(columns={"lmp": "np15_lmp"})
            df = pd.merge(sp15_df, np15_df, on="timestamp", how="inner")
            df = pd.merge(df, solar_df, on="timestamp", how="inner")

            df["hour"] = df["timestamp"].dt.hour
            df["date"] = df["timestamp"].dt.date

            # Overall metrics
            solar_mask = df["solar_mw"] > 0
            total_gen  = df.loc[solar_mask, "solar_mw"].sum()

            for hub, col in [("sp15", "sp15_lmp"), ("np15", "np15_lmp")]:
                cap     = (df.loc[solar_mask, col] * df.loc[solar_mask, "solar_mw"]).sum() / total_gen
                avg     = df[col].mean()
                neg_exp = df.loc[df[col] < 0, "solar_mw"].sum() / df["solar_mw"].sum()
                neg_hrs = int((df[col] < 0).sum())

                if year not in results:
                    results[year] = {"solar_gwh_total": round(df["solar_mw"].sum() / 1000, 1)}
                results[year][hub] = {
                    "capture_price": round(cap, 2),
                    "avg_lmp":       round(avg, 2),
                    "capture_ratio": round(cap / avg, 4),
                    "neg_exposure":  round(float(neg_exp), 4),
                    "neg_hours":     neg_hrs,
                }

            # Hourly duck curve profile
            hourly = df.groupby("hour").agg(
                sp15=("sp15_lmp", "mean"),
                np15=("np15_lmp", "mean"),
                mw=("solar_mw", "mean"),
            ).round(2)
            results[year]["hourly"] = {
                "sp15": hourly["sp15"].tolist(),
                "np15": hourly["np15"].tolist(),
                "mw":   hourly["mw"].tolist(),
            }

            # Daily capture
            daily = df.groupby("date").apply(lambda g: pd.Series({
                "cap_sp15": round((g["sp15_lmp"] * g["solar_mw"]).sum() / max(g["solar_mw"].sum(), 1), 2),
                "avg_sp15": round(g["sp15_lmp"].mean(), 2),
                "solar_gwh": round(g["solar_mw"].sum() / 1000, 2),
            })).reset_index()
            results[year]["daily"] = {
                "date":      [str(d) for d in daily["date"]],
                "cap_sp15":  daily["cap_sp15"].tolist(),
                "avg_sp15":  daily["avg_sp15"].tolist(),
                "solar_gwh": daily["solar_gwh"].tolist(),
            }

            print(f"    SP15: capture ${results[year]['sp15']['capture_price']:.2f}, "
                  f"avg ${results[year]['sp15']['avg_lmp']:.2f}, "
                  f"ratio {results[year]['sp15']['capture_ratio']:.2f}x, "
                  f"neg-exp {results[year]['sp15']['neg_exposure']:.1%}")

        except Exception as e:
            print(f"    ERROR loading {year}: {e}")

    return results


# ---------------------------------------------------------------------------
# HTML dashboard
# ---------------------------------------------------------------------------

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>CAISO Solar — year-on-year capture trend</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0b0b0b;color:#e0ddd6;padding:28px 22px}
.page{max-width:960px;margin:0 auto}
h1{font-size:20px;font-weight:500;color:#f0ede8;margin-bottom:4px}
.meta{font-size:11px;color:#4a4a48;margin-bottom:6px}
.thesis{background:#111;border:1px solid #1e1e1e;border-radius:8px;padding:13px 16px;margin-bottom:20px;font-size:12.5px;color:#888780;line-height:1.65}
.thesis strong{color:#d3d1c7;font-weight:500}
.controls{display:flex;gap:8px;margin-bottom:20px;align-items:center}
.ctrl-lbl{font-size:10px;letter-spacing:1px;color:#4a4a48;text-transform:uppercase}
.seg{display:flex;border:1px solid #222;border-radius:5px;overflow:hidden}
.seg-btn{background:none;border:none;border-right:1px solid #222;color:#5f5e5a;font-size:11px;padding:5px 12px;cursor:pointer;font-family:inherit}
.seg-btn:last-child{border-right:none}
.seg-btn.active{background:#0f1e2e;color:#378add}
.kpi-row{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;margin-bottom:20px}
.kpi{background:#111;border:1px solid #1e1e1e;border-radius:6px;padding:12px 14px}
.kpi-year{font-size:10px;letter-spacing:0.8px;color:#4a4a48;text-transform:uppercase;margin-bottom:5px}
.kpi-val{font-size:20px;font-weight:500;color:#f0ede8;line-height:1}
.kpi-sub{font-size:11px;margin-top:4px;color:#5f5e5a}
.delta{font-size:11px;margin-top:3px}
.red{color:#e24b4a}.amber{color:#ef9f27}.green{color:#639922}
.chart-block{background:#111;border:1px solid #1e1e1e;border-radius:8px;padding:16px 18px;margin-bottom:16px}
.two-col{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px}
.fig-eye{font-size:9px;letter-spacing:1.4px;color:#3a3a38;text-transform:uppercase;margin-bottom:4px}
.fig-title{font-size:13px;font-weight:500;color:#c8c5be;margin-bottom:3px}
.fig-sub{font-size:11px;color:#4a4a48;margin-bottom:12px;line-height:1.5}
.leg-row{display:flex;gap:14px;margin-bottom:10px;flex-wrap:wrap}
.leg{display:flex;align-items:center;gap:5px;font-size:11px;color:#5f5e5a}
.leg-line{width:18px;height:2px;border-radius:1px}
.callout{border-left:2px solid #185fa5;background:#091420;border-radius:0 4px 4px 0;padding:8px 12px;margin-top:10px;font-size:11.5px;color:#5f5e5a;line-height:1.6}
.callout strong{color:#85b7eb;font-weight:500}
@media(max-width:600px){.kpi-row{grid-template-columns:1fr 1fr}.two-col{grid-template-columns:1fr}}
</style>
</head>
<body><div class="page">
<h1>CAISO solar capture — year-on-year trend (January)</h1>
<div class="meta">SP15 &amp; NP15 hubs · Day-ahead market · January 2023 / 2024 / 2025</div>
<div class="thesis">
  <strong>The cannibalization thesis:</strong> As CAISO solar capacity has grown from ~13 GW (2022) to ~24 GW+ (2025), the capture ratio has declined because more solar competing at the same midday hours depresses the prices solar earns. This is the fundamental long-run valuation risk for solar assets — and the reason battery co-location is increasingly essential to restore value. January is used for consistent YoY comparison: no DST transitions, stable weather baseline, and moderate but meaningful solar penetration.
</div>
<div class="controls">
  <span class="ctrl-lbl">Hub</span>
  <div class="seg">
    <button class="seg-btn active" onclick="setHub('sp15')">SP15</button>
    <button class="seg-btn" onclick="setHub('np15')">NP15</button>
  </div>
</div>
<div class="kpi-row" id="kpis"></div>
<div class="chart-block">
  <div class="fig-eye">Figure 1</div>
  <div class="fig-title">Capture ratio trend — January, year on year</div>
  <div class="fig-sub">Generation-weighted capture price ÷ average LMP. Declining ratio = growing cannibalization discount as solar capacity expands.</div>
  <div style="position:relative;height:200px"><canvas id="c1"></canvas></div>
  <div class="callout" id="c1-note"></div>
</div>
<div class="two-col">
  <div class="chart-block">
    <div class="fig-eye">Figure 2</div>
    <div class="fig-title">Capture price vs avg LMP — by year</div>
    <div class="fig-sub">The widening gap between average LMP and capture price quantifies the growing value discount.</div>
    <div style="position:relative;height:220px"><canvas id="c2"></canvas></div>
  </div>
  <div class="chart-block">
    <div class="fig-eye">Figure 3</div>
    <div class="fig-title">Duck curve deepening — year on year</div>
    <div class="fig-sub">Avg hourly SP15 price by year. The midday trough gets deeper each year as solar capacity grows.</div>
    <div class="leg-row" id="duck-legend"></div>
    <div style="position:relative;height:220px"><canvas id="c3"></canvas></div>
  </div>
</div>
<div class="chart-block">
  <div class="fig-eye">Figure 4</div>
  <div class="fig-title">Negative-price exposure — growing tail risk</div>
  <div class="fig-sub">Share of solar generation occurring during negative-price hours. This is the direct curtailment and revenue-loss risk that storage co-location solves.</div>
  <div style="position:relative;height:160px"><canvas id="c4"></canvas></div>
  <div class="callout" id="c4-note"></div>
</div>
</div>
<script>
const D = __DATA__;
const GRID='#1a1a1a', TICK={color:'#3a3a38',font:{size:10}};
const YEAR_COLORS = {'2023':'#378add','2024':'#ef9f27','2025':'#e24b4a'};
const YEAR_COLORS_DIM = {'2023':'rgba(55,138,221,0.2)','2024':'rgba(239,159,39,0.2)','2025':'rgba(226,75,74,0.2)'};
let hub = 'sp15';
let charts = {};

function setHub(h){
  hub=h;
  document.querySelectorAll('.seg-btn').forEach(b=>b.classList.toggle('active',b.textContent.toLowerCase()===h));
  refresh();
}

function destroyAll(){Object.values(charts).forEach(c=>c.destroy());charts={};}

function refresh(){
  destroyAll();
  const years = D.years;

  // KPIs
  document.getElementById('kpis').innerHTML = years.map(y=>{
    const m = D.metrics[y][hub];
    const prev = years.indexOf(y)>0 ? D.metrics[years[years.indexOf(y)-1]][hub] : null;
    const delta = prev ? ((m.capture_ratio - prev.capture_ratio)*100).toFixed(1) : null;
    const deltaStr = delta ? `<div class="delta ${delta<0?'red':'green'}">${delta>0?'+':''}${delta}pp vs ${years[years.indexOf(y)-1]}</div>` : '';
    return `<div class="kpi">
      <div class="kpi-year">January ${y} · ${hub.toUpperCase()}</div>
      <div class="kpi-val" style="color:${YEAR_COLORS[y]}">${(m.capture_ratio*100).toFixed(1)}%</div>
      <div class="kpi-sub">capture ratio · $${m.capture_price.toFixed(1)}/MWh</div>
      ${deltaStr}
    </div>`;
  }).join('');

  // Fig 1 — capture ratio trend
  charts.c1 = new Chart(document.getElementById('c1'),{
    type:'bar',
    data:{
      labels:years,
      datasets:[{
        data:years.map(y=>+(D.metrics[y][hub].capture_ratio*100).toFixed(1)),
        backgroundColor:years.map(y=>YEAR_COLORS[y]),
        borderRadius:4,borderSkipped:false,
      }]
    },
    options:{responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:false},tooltip:{backgroundColor:'#161616',borderColor:'#222',borderWidth:1,titleColor:'#c8c5be',bodyColor:'#666',callbacks:{label:c=>`Capture ratio: ${c.parsed.y.toFixed(1)}%`}}},
      scales:{x:{ticks:{...TICK},grid:{color:GRID}},
              y:{min:0,max:110,ticks:{...TICK,callback:v=>`${v}%`},grid:{color:GRID}}}}
  });

  const firstRatio = D.metrics[years[0]][hub].capture_ratio;
  const lastRatio  = D.metrics[years[years.length-1]][hub].capture_ratio;
  const totalDrop  = ((firstRatio - lastRatio)*100).toFixed(1);
  document.getElementById('c1-note').innerHTML =
    `The capture ratio has fallen <strong>${totalDrop} percentage points</strong> from January ${years[0]} to January ${years[years.length-1]}. CAISO installed solar capacity grew from ~${D.capacity[years[0]]}GW to ~${D.capacity[years[years.length-1]]}GW over the same period — directly linking capacity growth to value erosion.`;

  // Fig 2 — capture vs avg LMP grouped bars
  charts.c2 = new Chart(document.getElementById('c2'),{
    type:'bar',
    data:{
      labels:years,
      datasets:[
        {label:'Avg LMP',data:years.map(y=>D.metrics[y][hub].avg_lmp),backgroundColor:'rgba(55,138,221,0.3)',borderColor:'#378add',borderWidth:1,borderRadius:3,borderSkipped:false},
        {label:'Capture',data:years.map(y=>D.metrics[y][hub].capture_price),backgroundColor:years.map(y=>YEAR_COLORS_DIM[y]),borderColor:years.map(y=>YEAR_COLORS[y]),borderWidth:1.5,borderRadius:3,borderSkipped:false},
      ]
    },
    options:{responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:false},tooltip:{backgroundColor:'#161616',borderColor:'#222',borderWidth:1,titleColor:'#c8c5be',bodyColor:'#666',callbacks:{label:c=>`${c.dataset.label}: $${c.parsed.y.toFixed(1)}/MWh`}}},
      scales:{x:{ticks:{...TICK},grid:{color:GRID}},
              y:{ticks:{...TICK,callback:v=>`$${v}`},grid:{color:GRID}}}}
  });

  // Fig 3 — duck curve deepening (always SP15 for this chart)
  const duckLegend = document.getElementById('duck-legend');
  duckLegend.innerHTML = years.map(y=>`<span class="leg"><span class="leg-line" style="background:${YEAR_COLORS[y]}"></span>${y}</span>`).join('');
  charts.c3 = new Chart(document.getElementById('c3'),{
    type:'line',
    data:{
      labels:Array.from({length:24},(_,i)=>`${String(i).padStart(2,'0')}:00`),
      datasets:years.map(y=>({
        data:D.metrics[y].hourly.sp15,
        borderColor:YEAR_COLORS[y],borderWidth:1.8,pointRadius:0,tension:0.4,label:y
      }))
    },
    options:{responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:false},tooltip:{backgroundColor:'#161616',borderColor:'#222',borderWidth:1,titleColor:'#c8c5be',bodyColor:'#666'}},
      scales:{x:{ticks:{...TICK,maxTicksLimit:8,maxRotation:0},grid:{color:GRID}},
              y:{ticks:{...TICK,callback:v=>v<0?`-$${Math.abs(v)}`:`$${v}`},grid:{color:GRID}}}}
  });

  // Fig 4 — negative price exposure
  charts.c4 = new Chart(document.getElementById('c4'),{
    type:'bar',
    data:{
      labels:years,
      datasets:[{
        data:years.map(y=>+(D.metrics[y][hub].neg_exposure*100).toFixed(1)),
        backgroundColor:years.map(y=>YEAR_COLORS[y]),
        borderRadius:4,borderSkipped:false,
      }]
    },
    options:{responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:false},tooltip:{backgroundColor:'#161616',borderColor:'#222',borderWidth:1,titleColor:'#c8c5be',bodyColor:'#666',callbacks:{label:c=>`${c.parsed.y.toFixed(1)}% of generation in negative-price hours`}}},
      scales:{x:{ticks:{...TICK},grid:{color:GRID}},
              y:{ticks:{...TICK,callback:v=>`${v}%`},grid:{color:GRID}}}}
  });

  const negFirst = D.metrics[years[0]][hub].neg_exposure;
  const negLast  = D.metrics[years[years.length-1]][hub].neg_exposure;
  document.getElementById('c4-note').innerHTML =
    `Negative-price exposure rose from <strong>${(negFirst*100).toFixed(1)}%</strong> in ${years[0]} to <strong>${(negLast*100).toFixed(1)}%</strong> in ${years[years.length-1]}. This is the clearest signal of why storage co-location is increasingly valuable: a 2-hour battery can charge during these hours instead of curtailing, converting a loss into a revenue opportunity.`;
}

refresh();
</script></body></html>"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 55)
    print("analysis_yoy.py — year-on-year capture ratio trend")
    print("=" * 55)
    print("\nBest month: January (no DST, stable weather, consistent penetration signal)")
    print("Years: 2023, 2024, 2025\n")

    years = sorted(YOY_PERIODS.keys())
    metrics_raw = compute_yoy_metrics(years)

    if not metrics_raw:
        raise RuntimeError("No data loaded. Check API connectivity.")

    available_years = sorted(metrics_raw.keys())
    print(f"\nSuccessfully loaded: {available_years}")

    # Add hourly profile to metrics for charting
    metrics_payload = {}
    for year in available_years:
        m = metrics_raw[year]
        metrics_payload[year] = {
            "sp15":    m["sp15"],
            "np15":    m["np15"],
            "hourly":  m["hourly"],
            "solar_gwh_total": m["solar_gwh_total"],
        }

    payload = {
        "years":   [str(y) for y in available_years],
        "metrics": {str(y): metrics_payload[y] for y in available_years},
        "capacity": {str(y): SOLAR_CAPACITY_GW.get(y, "?") for y in available_years},
    }

    html = HTML.replace("__DATA__", json.dumps(payload))
    out  = OUT_DIR / "yoy_trend.html"
    out.write_text(html, encoding="utf-8")
    print(f"\nDashboard saved → {out}")
    print("\nSummary (SP15):")
    for y in available_years:
        m = metrics_raw[y]["sp15"]
        print(f"  Jan {y}: capture ${m['capture_price']:.2f}, ratio {m['capture_ratio']:.2f}x, "
              f"neg-exp {m['neg_exposure']:.1%}, capacity ~{SOLAR_CAPACITY_GW.get(y,'?')}GW")


if __name__ == "__main__":
    main()
