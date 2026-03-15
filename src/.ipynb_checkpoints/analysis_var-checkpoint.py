"""
analysis_var.py
---------------
Value-at-risk framing for solar capture price.

Developers and lenders don't just care about the average capture ratio —
they care about downside scenarios. A project with a mean capture ratio of
0.75x but a P10 of 0.40x is a very different risk profile from one with
a P10 of 0.65x.

This script computes:
  - P10 / P50 / P90 capture ratio distribution across daily observations
  - Distribution of capture price by month and by quartile of solar generation
  - "Bad day" anatomy: what does a P10 day look like hourly?
  - VaR summary: how many days fell below key thresholds (0.5x, 0.6x, 0.75x)?

These are the metrics a project finance analyst or PPA pricing team would
use to stress-test a solar revenue forecast.

Output:
    outputs/value_at_risk.html

Run: python src/analysis_var.py
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


# ---------------------------------------------------------------------------
# Metric computation
# ---------------------------------------------------------------------------

def compute_daily_metrics(df: pd.DataFrame, lmp_col: str = "sp15_lmp") -> pd.DataFrame:
    df = df.copy()
    df["date"]  = df["timestamp"].dt.date
    df["month"] = df["timestamp"].dt.strftime("%Y-%m")

    daily = df.groupby("date").apply(lambda g: pd.Series({
        "capture_price": (g[lmp_col] * g["solar_mw"]).sum() / max(g["solar_mw"].sum(), 1),
        "avg_lmp":       g[lmp_col].mean(),
        "solar_gwh":     g["solar_mw"].sum() / 1000,
        "neg_hours":     int((g[lmp_col] < 0).sum()),
        "month":         g["month"].iloc[0],
    })).reset_index()

    daily["date"]          = pd.to_datetime(daily["date"])
    daily["capture_ratio"] = daily["capture_price"] / daily["avg_lmp"].replace(0, np.nan)
    return daily.dropna(subset=["capture_ratio"])


def compute_percentiles(daily: pd.DataFrame) -> dict:
    """Full distribution stats for the capture ratio."""
    ratios = daily["capture_ratio"].dropna().values
    pcts   = [5, 10, 25, 50, 75, 90, 95]
    return {
        "percentiles": {f"p{p}": round(float(np.percentile(ratios, p)), 4) for p in pcts},
        "mean":  round(float(ratios.mean()), 4),
        "std":   round(float(ratios.std()), 4),
        "min":   round(float(ratios.min()), 4),
        "max":   round(float(ratios.max()), 4),
        "n_days": int(len(ratios)),
    }


def compute_threshold_var(daily: pd.DataFrame) -> dict:
    """How many days fell below key capture ratio thresholds?"""
    ratios = daily["capture_ratio"].dropna()
    total  = len(ratios)
    thresholds = [0.40, 0.50, 0.60, 0.70, 0.75, 0.80, 0.90]
    return {
        str(t): {
            "n_days": int((ratios < t).sum()),
            "pct":    round(float((ratios < t).mean()), 4),
        }
        for t in thresholds
    }


def get_bad_day_profile(df: pd.DataFrame, daily: pd.DataFrame,
                        lmp_col: str = "sp15_lmp", percentile: int = 10) -> dict:
    """
    Average hourly price and generation profile on P10 (bad) days
    vs P90 (good) days — shows structurally what drives the tail risk.
    """
    p10_threshold = np.percentile(daily["capture_ratio"].dropna(), percentile)
    p90_threshold = np.percentile(daily["capture_ratio"].dropna(), 100 - percentile)

    bad_dates  = set(daily.loc[daily["capture_ratio"] <= p10_threshold, "date"].dt.date)
    good_dates = set(daily.loc[daily["capture_ratio"] >= p90_threshold, "date"].dt.date)

    df = df.copy()
    df["date"] = df["timestamp"].dt.date
    df["hour"] = df["timestamp"].dt.hour

    bad_df  = df[df["date"].isin(bad_dates)]
    good_df = df[df["date"].isin(good_dates)]

    def profile(g):
        return g.groupby("hour").agg(
            avg_lmp=(lmp_col, "mean"),
            avg_mw=("solar_mw", "mean"),
        ).round(2)

    bad_prof  = profile(bad_df)
    good_prof = profile(good_df)

    return {
        "hours":         list(range(24)),
        "bad_lmp":       [round(bad_prof["avg_lmp"].get(h, 0), 2)  for h in range(24)],
        "good_lmp":      [round(good_prof["avg_lmp"].get(h, 0), 2) for h in range(24)],
        "bad_mw":        [round(bad_prof["avg_mw"].get(h, 0) / 1000, 2)  for h in range(24)],
        "good_mw":       [round(good_prof["avg_mw"].get(h, 0) / 1000, 2) for h in range(24)],
        "p10_threshold": round(p10_threshold, 4),
        "p90_threshold": round(p90_threshold, 4),
        "n_bad_days":    len(bad_dates),
        "n_good_days":   len(good_dates),
    }


def histogram_bins(daily: pd.DataFrame, n_bins: int = 20) -> dict:
    """Histogram of daily capture ratios for distribution chart."""
    ratios = daily["capture_ratio"].dropna().values
    counts, edges = np.histogram(ratios, bins=n_bins, range=(0, 1.5))
    centers = [(edges[i] + edges[i+1]) / 2 for i in range(len(counts))]
    return {
        "centers": [round(c, 3) for c in centers],
        "counts":  counts.tolist(),
        "bin_width": round(edges[1] - edges[0], 3),
    }


# ---------------------------------------------------------------------------
# HTML dashboard
# ---------------------------------------------------------------------------

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Solar capture — value at risk</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0b0b0b;color:#e0ddd6;padding:28px 22px}
.page{max-width:960px;margin:0 auto}
h1{font-size:20px;font-weight:500;color:#f0ede8;margin-bottom:4px}
.meta{font-size:13px;color:#9a9890;margin-bottom:20px}
.kpi-row{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:10px;margin-bottom:20px}
.kpi{background:#111;border:1px solid #1e1e1e;border-radius:6px;padding:12px 14px}
.kpi-lbl{font-size:13px;letter-spacing:0.9px;color:#9a9890;text-transform:uppercase;margin-bottom:7px}
.kpi-val{font-size:20px;font-weight:500;color:#f0ede8;line-height:1}
.kpi-sub{font-size:13px;margin-top:5px;color:#b0aea8}
.red{color:#e24b4a}.amber{color:#ef9f27}.green{color:#639922}
.chart-block{background:#111;border:1px solid #1e1e1e;border-radius:8px;padding:16px 18px;margin-bottom:16px}
.two-col{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px}
.fig-eye{font-size:13px;letter-spacing:1.4px;color:#8a8880;text-transform:uppercase;margin-bottom:4px}
.fig-title{font-size:13px;font-weight:500;color:#c8c5be;margin-bottom:3px}
.fig-sub{font-size:13px;color:#9a9890;margin-bottom:12px;line-height:1.5}
.leg-row{display:flex;gap:14px;margin-bottom:10px;flex-wrap:wrap}
.leg{display:flex;align-items:center;gap:5px;font-size:13px;color:#b0aea8}
.leg-line{width:18px;height:2px;border-radius:1px}
.leg-sq{width:10px;height:10px;border-radius:2px}
.callout{border-left:2px solid #854f0b;background:#1a1200;border-radius:0 4px 4px 0;padding:8px 12px;margin-top:10px;font-size:13px;color:#b0aea8;line-height:1.6}
.callout strong{color:#ef9f27;font-weight:500}
.var-table{width:100%;border-collapse:collapse;font-size:12px;margin-top:12px}
.var-table th{font-size:12px;letter-spacing:0.8px;color:#9a9890;text-transform:uppercase;padding:6px 10px;border-bottom:1px solid #1e1e1e;text-align:left}
.var-table td{padding:7px 10px;border-bottom:1px solid #161616;color:#c8c5be}
.var-table td:last-child{text-align:right}
.var-table tr.highlight td{color:#ef9f27}
.bar-cell{position:relative;padding-left:8px}
.bar-fill{height:8px;border-radius:2px;background:#e24b4a;opacity:0.6;display:inline-block;vertical-align:middle;margin-right:6px}
@media(max-width:600px){.kpi-row{grid-template-columns:repeat(3,minmax(0,1fr))}.two-col{grid-template-columns:1fr}}
</style>
</head>
<body><div class="page">
<h1>Solar capture — value at risk analysis</h1>
<div class="meta" id="meta"></div>
<div class="kpi-row" id="kpis"></div>

<div class="two-col">
  <div class="chart-block">
    <div class="fig-eye">Figure 1</div>
    <div class="fig-title">Daily capture ratio — distribution</div>
    <div class="fig-sub">Histogram of daily capture ratios. P10 = worst 10% of days. The left tail is the developer's risk exposure.</div>
    <div style="position:relative;height:200px"><canvas id="c1"></canvas></div>
    <div class="callout" id="c1-note"></div>
  </div>
  <div class="chart-block">
    <div class="fig-eye">Figure 2</div>
    <div class="fig-title">Daily capture ratio — time series</div>
    <div class="fig-sub">Each bar is one day. P10 and P90 reference lines show the range of outcomes.</div>
    <div style="position:relative;height:200px"><canvas id="c2"></canvas></div>
  </div>
</div>

<div class="chart-block">
  <div class="fig-eye">Figure 3</div>
  <div class="fig-title">Bad day vs good day anatomy — hourly profile</div>
  <div class="fig-sub">Average hourly price on P10 (worst) days vs P90 (best) days. Shows <em>why</em> bad days are bad: higher solar output pushing prices deeper negative during the same peak hours.</div>
  <div class="leg-row">
    <span class="leg"><span class="leg-line" style="background:#e24b4a"></span>P10 days — avg price</span>
    <span class="leg"><span class="leg-line" style="background:#639922"></span>P90 days — avg price</span>
    <span class="leg"><span class="leg-sq" style="background:#1a1a1a;border:1px solid #e24b4a"></span>P10 solar gen</span>
    <span class="leg"><span class="leg-sq" style="background:#1a2e12"></span>P90 solar gen</span>
  </div>
  <div style="position:relative;height:240px"><canvas id="c3"></canvas></div>
  <div class="callout" id="c3-note"></div>
</div>

<div class="chart-block">
  <div class="fig-eye">Figure 4</div>
  <div class="fig-title">Threshold exceedance — days below key capture ratio levels</div>
  <div class="fig-sub">How often did solar assets earn less than key thresholds? Critical for PPA pricing and project finance stress tests.</div>
  <div id="var-table-wrap"></div>
</div>

</div>
<script>
const D = __DATA__;
const GRID='#1a1a1a', TICK={color:'#8a8880',font:{size:12}};
const p = D.stats.percentiles;

document.getElementById('meta').textContent = D.period + ' · SP15 hub · daily capture ratio distribution';

const pColor = v => v < 0.5 ? '#e24b4a' : v < 0.7 ? '#ef9f27' : '#639922';
document.getElementById('kpis').innerHTML = [
  {l:'P10 capture ratio', v:(p.p10*100).toFixed(1)+'%', s:'Worst 10% of days', c:pColor(p.p10)},
  {l:'P25 capture ratio', v:(p.p25*100).toFixed(1)+'%', s:'Lower quartile', c:pColor(p.p25)},
  {l:'P50 / median', v:(p.p50*100).toFixed(1)+'%', s:'Typical day', c:pColor(p.p50)},
  {l:'P75 capture ratio', v:(p.p75*100).toFixed(1)+'%', s:'Upper quartile', c:pColor(p.p75)},
  {l:'P90 capture ratio', v:(p.p90*100).toFixed(1)+'%', s:'Best 10% of days', c:pColor(p.p90)},
].map(k=>`<div class="kpi"><div class="kpi-lbl">${k.l}</div><div class="kpi-val" style="color:${k.c}">${k.v}</div><div class="kpi-sub">${k.s}</div></div>`).join('');

// Fig 1 — histogram
const h = D.histogram;
new Chart(document.getElementById('c1'),{
  type:'bar',
  data:{labels:h.centers.map(c=>(c*100).toFixed(0)+'%'),
        datasets:[{data:h.counts,backgroundColor:h.centers.map(c=>c<p.p10?'rgba(226,75,74,0.7)':c<p.p50?'rgba(239,159,39,0.5)':'rgba(99,153,34,0.45)'),borderRadius:2,borderSkipped:false}]},
  options:{responsive:true,maintainAspectRatio:false,
    plugins:{legend:{display:false},tooltip:{backgroundColor:'#161616',borderColor:'#222',borderWidth:1,titleColor:'#c8c5be',bodyColor:'#666',callbacks:{title:i=>`Capture ratio ~${i[0].label}`,label:c=>`${c.parsed.y} days`}}},
    scales:{x:{ticks:{...TICK,maxTicksLimit:8},grid:{color:GRID},title:{display:true,text:'Daily capture ratio',color:'#8a8880',font:{size:12}}},
            y:{ticks:{...TICK,callback:v=>`${v}d`},grid:{color:GRID},title:{display:true,text:'Days',color:'#8a8880',font:{size:12}}}}}
});
document.getElementById('c1-note').innerHTML =
  `P10 days (red bars) have capture ratios below <strong>${(p.p10*100).toFixed(1)}%</strong>. The spread from P10 to P90 is <strong>${((p.p90-p.p10)*100).toFixed(1)} percentage points</strong> — a wide distribution signals high revenue uncertainty for project lenders.`;

// Fig 2 — time series
new Chart(document.getElementById('c2'),{
  type:'bar',
  data:{labels:D.daily.date.map(d=>d.slice(5)),
        datasets:[{data:D.daily.capture_ratio.map(v=>+(v*100).toFixed(1)),
          backgroundColor:D.daily.capture_ratio.map(v=>v<p.p10?'#e24b4a':v<p.p50?'#ef9f27':'#3b6d11'),borderRadius:1,borderSkipped:false}]},
  options:{responsive:true,maintainAspectRatio:false,
    plugins:{legend:{display:false},tooltip:{backgroundColor:'#161616',borderColor:'#222',borderWidth:1,titleColor:'#c8c5be',bodyColor:'#666',callbacks:{label:c=>`Capture ratio: ${c.parsed.y.toFixed(1)}%`}}},
    scales:{x:{ticks:{...TICK,maxTicksLimit:10,maxRotation:0},grid:{color:GRID}},
            y:{ticks:{...TICK,callback:v=>`${v}%`},grid:{color:GRID},
              afterDataLimits:a=>{a.min=0;a.max=Math.max(120,a.max)}}}}
});

// Fig 3 — bad day vs good day anatomy
const bd = D.bad_day;
new Chart(document.getElementById('c3'),{
  data:{
    labels:Array.from({length:24},(_,i)=>`${String(i).padStart(2,'0')}:00`),
    datasets:[
      {type:'line',data:bd.bad_lmp,borderColor:'#e24b4a',borderWidth:2,pointRadius:0,tension:0.3,yAxisID:'y'},
      {type:'line',data:bd.good_lmp,borderColor:'#639922',borderWidth:2,pointRadius:0,tension:0.3,yAxisID:'y'},
      {type:'bar',data:bd.bad_mw,backgroundColor:'rgba(226,75,74,0.15)',borderColor:'rgba(226,75,74,0.3)',borderWidth:0.5,yAxisID:'y2',borderRadius:1},
      {type:'bar',data:bd.good_mw,backgroundColor:'rgba(99,153,34,0.15)',borderColor:'rgba(99,153,34,0.3)',borderWidth:0.5,yAxisID:'y2',borderRadius:1},
    ]
  },
  options:{responsive:true,maintainAspectRatio:false,
    plugins:{legend:{display:false},tooltip:{backgroundColor:'#161616',borderColor:'#222',borderWidth:1,titleColor:'#c8c5be',bodyColor:'#666'}},
    scales:{x:{ticks:{...TICK,maxTicksLimit:8,maxRotation:0},grid:{color:GRID}},
            y:{position:'left',ticks:{...TICK,callback:v=>v<0?`-$${Math.abs(v)}`:`$${v}`},grid:{color:GRID}},
            y2:{position:'right',ticks:{...TICK,callback:v=>`${v}GW`},grid:{display:false}}}}
});
const badPeak  = Math.min(...bd.bad_lmp.filter((_,i)=>bd.bad_mw[i]>0.5));
const goodPeak = Math.min(...bd.good_lmp.filter((_,i)=>bd.good_mw[i]>0.5));
document.getElementById('c3-note').innerHTML =
  `On P10 days, midday prices hit <strong>$${badPeak.toFixed(0)}/MWh</strong> during solar peak hours. On P90 days the same hours average <strong>$${goodPeak.toFixed(0)}/MWh</strong>. The difference in hourly price — not generation volume — is what separates good from bad capture days.`;

// Fig 4 — threshold table
const tv = D.threshold_var;
const thresholds = Object.keys(tv).map(Number).sort((a,b)=>a-b);
const maxPct = Math.max(...thresholds.map(t=>tv[t].pct));
document.getElementById('var-table-wrap').innerHTML = `
  <table class="var-table">
    <thead><tr><th>Capture ratio threshold</th><th>Days below</th><th>% of days</th><th>Risk level</th><th style="width:140px">Frequency bar</th></tr></thead>
    <tbody>${thresholds.map(t=>{
      const {n_days,pct}=tv[String(t)];
      const risk = t<=0.5?'Severe':t<=0.65?'High':t<=0.75?'Elevated':'Moderate';
      const col  = t<=0.5?'#e24b4a':t<=0.65?'#e24b4a':t<=0.75?'#ef9f27':'#b0aea8';
      const w    = Math.round((pct/Math.max(maxPct,0.01))*100);
      const hl   = t==0.75?' class="highlight"':'';
      return `<tr${hl}><td>Below ${(t*100).toFixed(0)}%</td><td>${n_days} days</td><td>${(pct*100).toFixed(1)}%</td><td style="color:${col}">${risk}</td><td class="bar-cell"><span class="bar-fill" style="width:${w}px;background:${col}"></span>${(pct*100).toFixed(1)}%</td></tr>`;
    }).join('')}</tbody>
  </table>`;
</script></body></html>"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 50)
    print("analysis_var.py — value-at-risk capture distribution")
    print("=" * 50)

    df = pd.read_csv(PROC_DIR / "market_data.csv")
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    if df["timestamp"].dt.tz is None:
        df["timestamp"] = df["timestamp"].dt.tz_localize(PACIFIC)

    # Filter to full months only (drop partial months at boundaries)
    df["month"] = df["timestamp"].dt.to_period("M")
    month_counts = df.groupby("month").size()
    full_months  = month_counts[month_counts >= 24 * 25].index  # at least 25 days
    df = df[df["month"].isin(full_months)].copy()

    print(f"Using {len(full_months)} full month(s): {[str(m) for m in full_months]}")

    daily   = compute_daily_metrics(df)
    stats   = compute_percentiles(daily)
    thresh  = compute_threshold_var(daily)
    bd      = get_bad_day_profile(df, daily)
    hist    = histogram_bins(daily)

    print(f"\nCapture ratio distribution:")
    for k, v in stats["percentiles"].items():
        print(f"  {k.upper()}: {v:.1%}")
    print(f"  Mean:  {stats['mean']:.1%}")
    print(f"  Std:   {stats['std']:.1%}")

    print(f"\nThreshold exceedance:")
    for t, d in thresh.items():
        print(f"  Below {float(t):.0%}: {d['n_days']} days ({d['pct']:.1%})")

    payload = {
        "period": f"{daily['date'].min().strftime('%b %Y')} – {daily['date'].max().strftime('%b %Y')}",
        "stats":  stats,
        "threshold_var": thresh,
        "bad_day": bd,
        "histogram": hist,
        "daily": {
            "date":          [str(d.date()) for d in daily["date"]],
            "capture_ratio": daily["capture_ratio"].round(4).tolist(),
            "capture_price": daily["capture_price"].round(2).tolist(),
            "solar_gwh":     daily["solar_gwh"].round(2).tolist(),
        }
    }

    html = HTML.replace("__DATA__", json.dumps(payload))
    out  = OUT_DIR / "value_at_risk.html"
    out.write_text(html, encoding="utf-8")
    print(f"\nDashboard saved → {out}")


if __name__ == "__main__":
    main()
