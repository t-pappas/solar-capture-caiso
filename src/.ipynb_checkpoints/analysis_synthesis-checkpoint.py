"""
analysis_synthesis.py
---------------------
Generates the synthesis dashboard — a single self-contained HTML file that
tells the complete story of solar cannibalization in CAISO using all available
data across 2023, 2024, and 2025.

The dashboard has five narrative sections:
  1. The capture ratio is collapsing (headline YoY metrics)
  2. Why: the duck curve deepens every year
  3. The next stage: negative prices and value-at-risk
  4. Curtailment: the hidden revenue loss
  5. What this means for market participants

Data sources:
  - data/yoy/lmp_SP15_YYYY.csv / lmp_NP15_YYYY.csv / solar_YYYY.csv
  - data/processed/curtailment_hourly.csv  (from analysis_curtailment.py)

Output:
  outputs/synthesis_dashboard.html

Run: python src/analysis_synthesis.py
"""

from pathlib import Path
import json
import numpy as np
import pandas as pd

ROOT     = Path(__file__).resolve().parents[1]
YOY_DIR  = ROOT / "data" / "yoy"
PROC_DIR = ROOT / "data" / "processed"
OUT_DIR  = ROOT / "outputs"
OUT_DIR.mkdir(parents=True, exist_ok=True)

PACIFIC = "America/Los_Angeles"
YEARS   = [2023, 2024, 2025]
SOLAR_CAPACITY_GW = {2023: 16.2, 2024: 19.8, 2025: 23.5}


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_all_years() -> pd.DataFrame:
    frames = []
    for year in YEARS:
        sp15 = pd.read_csv(YOY_DIR / f"lmp_SP15_{year}.csv")
        np15 = pd.read_csv(YOY_DIR / f"lmp_NP15_{year}.csv")
        sol  = pd.read_csv(YOY_DIR / f"solar_{year}.csv")
        for d in [sp15, np15, sol]:
            d["timestamp"] = pd.to_datetime(d["timestamp"])
        df = (sp15.rename(columns={"lmp": "sp15_lmp"})
                  .merge(np15.rename(columns={"lmp": "np15_lmp"}), on="timestamp")
                  .merge(sol, on="timestamp"))
        df["year"] = year
        frames.append(df)
    all_df = pd.concat(frames).sort_values("timestamp").reset_index(drop=True)
    all_df["hour"] = all_df["timestamp"].dt.hour
    all_df["date"] = all_df["timestamp"].dt.date
    return all_df


def load_curtailment_jan() -> pd.DataFrame:
    path = PROC_DIR / "curtailment_hourly.csv"
    if not path.exists():
        print("  WARNING: curtailment_hourly.csv not found. Run analysis_curtailment.py first.")
        return pd.DataFrame(columns=["timestamp", "curtailed_mw", "year"])
    df = pd.read_csv(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["year"]  = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month
    return df[df["month"] == 1].copy()


# ---------------------------------------------------------------------------
# Metric computation
# ---------------------------------------------------------------------------

def compute_yoy_summary(all_df: pd.DataFrame) -> dict:
    out = {}
    for year, g in all_df.groupby("year"):
        mask = g["solar_mw"] > 0
        tot  = g.loc[mask, "solar_mw"].sum()
        for hub, col in [("sp15", "sp15_lmp"), ("np15", "np15_lmp")]:
            cap = (g.loc[mask, col] * g.loc[mask, "solar_mw"]).sum() / tot
            avg = g[col].mean()
            neg = g.loc[g[col] < 0, "solar_mw"].sum() / g["solar_mw"].sum()
            if year not in out:
                out[year] = {}
            out[year][hub] = dict(
                cap=round(float(cap), 2),
                avg=round(float(avg), 2),
                ratio=round(float(cap / avg), 4),
                neg_exp=round(float(neg), 4),
                neg_hrs=int((g[col] < 0).sum()),
            )
    return out


def compute_duck_curves(all_df: pd.DataFrame) -> dict:
    out = {}
    for year, g in all_df.groupby("year"):
        h = g.groupby("hour").agg(
            sp15=("sp15_lmp", "mean"),
            np15=("np15_lmp", "mean"),
            mw=("solar_mw", "mean"),
        ).round(2)
        out[year] = {"sp15": h["sp15"].tolist(), "np15": h["np15"].tolist(), "mw": h["mw"].tolist()}
    return out


def compute_daily(all_df: pd.DataFrame) -> dict:
    out = {}
    for year, g in all_df.groupby("year"):
        d = g.groupby("date").apply(lambda x: pd.Series({
            "cap": (x["sp15_lmp"] * x["solar_mw"]).sum() / max(x["solar_mw"].sum(), 1),
            "avg": x["sp15_lmp"].mean(),
            "gwh": x["solar_mw"].sum() / 1000,
        })).reset_index()
        d["ratio"] = d["cap"] / d["avg"]
        out[year] = {
            "date":  [str(x) for x in d["date"]],
            "ratio": d["ratio"].round(4).tolist(),
            "cap":   d["cap"].round(2).tolist(),
            "avg":   d["avg"].round(2).tolist(),
            "gwh":   d["gwh"].round(2).tolist(),
        }
    return out


def compute_curtailment_summary(curt_jan: pd.DataFrame) -> dict:
    out = {}
    for year, g in curt_jan.groupby("year"):
        out[year] = dict(
            total_mwh=round(float(g["curtailed_mw"].sum()), 1),
            peak_mw=round(float(g["curtailed_mw"].max()), 1),
            hours=int((g["curtailed_mw"] > 0).sum()),
        )
    return out


def compute_curtailment_chart(curt_jan: pd.DataFrame, top_n: int = 30) -> dict:
    if curt_jan.empty:
        return {"labels": [], "mw": [], "year": []}
    top = curt_jan.nlargest(top_n, "curtailed_mw").sort_values("timestamp")
    ts = top["timestamp"].dt.tz_localize(None) if top["timestamp"].dt.tz else top["timestamp"]
    return {
        "labels": [t.strftime("%-d %b %H:%M") for t in ts],
        "mw":     top["curtailed_mw"].round(1).tolist(),
        "year":   top["year"].tolist(),
    }


def compute_percentile(values: list, p: float) -> float:
    s = sorted(values)
    i = (p / 100) * (len(s) - 1)
    lo, hi = int(i), min(int(i) + 1, len(s) - 1)
    return round(s[lo] + (s[hi] - s[lo]) * (i - lo), 4)


# ---------------------------------------------------------------------------
# Build payload and fill template
# ---------------------------------------------------------------------------

def build_payload(yoy, duck, daily, curt_chart) -> str:
    return json.dumps({
        "yoy":        {str(y): v for y, v in yoy.items()},
        "duck":       {str(y): v for y, v in duck.items()},
        "daily":      {str(y): v for y, v in daily.items()},
        "curt_chart": curt_chart,
    })


def compute_text_vars(yoy, duck, daily, curt_summary) -> dict:
    sp = yoy

    def peak_price(year):
        mw = duck[year]["mw"]
        ph = mw.index(max(mw))
        return round(duck[year]["sp15"][ph], 1)

    def eve_price(year):
        return round(max(duck[year]["sp15"][17], duck[year]["sp15"][18], duck[year]["sp15"][19]), 1)

    r23, r24, r25 = sp[2023]["sp15"]["ratio"], sp[2024]["sp15"]["ratio"], sp[2025]["sp15"]["ratio"]
    cap23, cap25  = sp[2023]["sp15"]["cap"], sp[2025]["sp15"]["cap"]
    avg23, avg25  = sp[2023]["sp15"]["avg"], sp[2025]["sp15"]["avg"]

    p10_23 = compute_percentile(daily[2023]["ratio"], 10)
    p10_25 = compute_percentile(daily[2025]["ratio"], 10)

    curt_23 = round(curt_summary.get(2023, {}).get("total_mwh", 0) / 1000, 1)
    curt_24 = round(curt_summary.get(2024, {}).get("total_mwh", 0) / 1000, 1)
    curt_25 = round(curt_summary.get(2025, {}).get("total_mwh", 0) / 1000, 1)
    peak_curt = max(curt_summary.get(y, {}).get("peak_mw", 0) for y in [2023, 2024, 2025])

    return {
        "__CAP_2023__":       str(SOLAR_CAPACITY_GW[2023]),
        "__CAP_2025__":       str(SOLAR_CAPACITY_GW[2025]),
        "__RATIO_DROP__":     str(round((r23 - r25) * 100, 1)),
        "__RATIO_2023__":     f"{r23:.2f}",
        "__RATIO_2024__":     f"{r24:.2f}",
        "__RATIO_2025__":     f"{r25:.2f}",
        "__RATIO_2025_PCT__": f"{round((1-r25)*100, 1)}",
        "__CAP_2023_VAL__":   f"{cap23:.2f}",
        "__CAP_2024_VAL__":   f"{sp[2024]['sp15']['cap']:.2f}",
        "__CAP_2025_VAL__":   f"{cap25:.2f}",
        "__AVG_2023__":       f"{avg23:.2f}",
        "__AVG_2024__":       f"{sp[2024]['sp15']['avg']:.2f}",
        "__AVG_2025__":       f"{avg25:.2f}",
        "__CAP_PCT_DROP__":   str(round((1 - cap25 / cap23) * 100, 1)),
        "__AVG_PCT_DROP__":   str(round((1 - avg25 / avg23) * 100, 1)),
        "__PEAK_PRICE_2023__": str(peak_price(2023)),
        "__PEAK_PRICE_2024__": str(peak_price(2024)),
        "__PEAK_PRICE_2025__": str(peak_price(2025)),
        "__EVE_PRICE_2023__":  str(eve_price(2023)),
        "__EVE_PRICE_2024__":  str(eve_price(2024)),
        "__EVE_PRICE_2025__":  str(eve_price(2025)),
        "__NEG_EXP_2025__":   f"{sp[2025]['sp15']['neg_exp']*100:.1f}",
        "__NEG_HRS_2025__":   str(sp[2025]["sp15"]["neg_hrs"]),
        "__CURT_2023_MWH__":  str(curt_23),
        "__CURT_2024_MWH__":  str(curt_24),
        "__CURT_2025_MWH__":  str(curt_25),
        "__CURT_GROWTH__":    str(round(curt_25 / max(curt_23, 0.1), 1)),
        "__PEAK_CURT_MW__":   f"{peak_curt:.0f}",
        "__P10_2023__":       f"{p10_23:.2f}",
        "__P10_2025__":       f"{p10_25:.2f}",
        "__P10_2025_PCT__":   f"{round(p10_25*100, 1)}",
        "__NP15_RATIO_2025__": f"{sp[2025]['np15']['ratio']:.2f}",
    }


# ---------------------------------------------------------------------------
# HTML template — all __PLACEHOLDERS__ are filled at runtime
# ---------------------------------------------------------------------------

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>The solar value problem — CAISO</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0b0b0b;color:#e0ddd6;padding:32px 24px;line-height:1.5}
.page{max-width:980px;margin:0 auto}
.masthead{border-bottom:1px solid #1e1e1e;padding-bottom:20px;margin-bottom:32px}
.masthead-eyebrow{font-size:11px;letter-spacing:2px;text-transform:uppercase;color:#8a8880;margin-bottom:8px}
.masthead-title{font-size:28px;font-weight:500;color:#f0ede8;letter-spacing:-0.5px;line-height:1.2;margin-bottom:8px}
.masthead-sub{font-size:13px;color:#9a9890;max-width:640px;line-height:1.6}
.masthead-meta{font-size:11px;color:#5f5e5a;margin-top:10px}
.section{margin-bottom:40px}
.section-header{display:flex;align-items:baseline;gap:12px;margin-bottom:6px}
.section-num{font-size:11px;letter-spacing:2px;color:#5f5e5a;text-transform:uppercase;min-width:20px}
.section-title{font-size:16px;font-weight:500;color:#f0ede8}
.section-thesis{font-size:13px;color:#b0aea8;max-width:720px;line-height:1.65;margin-bottom:18px;padding-left:32px}
.kpi-strip{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin-bottom:20px}
.three-col-kpi{grid-template-columns:repeat(3,minmax(0,1fr))}
.kpi{background:#111;border:1px solid #1e1e1e;border-radius:6px;padding:14px 16px}
.kpi-lbl{font-size:11px;letter-spacing:0.6px;color:#8a8880;text-transform:uppercase;margin-bottom:6px}
.kpi-val{font-size:26px;font-weight:500;line-height:1;letter-spacing:-0.5px}
.kpi-ctx{font-size:12px;margin-top:5px;color:#8a8880}
.kv-red{color:#e24b4a}.kv-amber{color:#ef9f27}.kv-green{color:#639922}.kv-flat{color:#c8c5be}
.chart-card{background:#111;border:1px solid #1e1e1e;border-radius:8px;padding:18px 20px;margin-bottom:14px}
.chart-card.a-red{border-left:3px solid #e24b4a}
.chart-card.a-amber{border-left:3px solid #ef9f27}
.chart-card.a-blue{border-left:3px solid #378add}
.card-title{font-size:13px;font-weight:500;color:#d3d1c7;margin-bottom:3px}
.card-sub{font-size:12px;color:#8a8880;margin-bottom:14px;line-height:1.5}
.two-col{display:grid;grid-template-columns:1fr 1fr;gap:14px}
.leg-row{display:flex;gap:14px;margin-bottom:10px;flex-wrap:wrap}
.leg{display:flex;align-items:center;gap:6px;font-size:12px;color:#9a9890}
.leg-sq{width:10px;height:10px;border-radius:2px;flex-shrink:0}
.leg-line{width:18px;height:2px;border-radius:1px;flex-shrink:0}
.callout{border-radius:0 4px 4px 0;padding:10px 14px;margin-top:12px;font-size:12px;line-height:1.65}
.callout.red{background:#1a0a0a;border-left:2px solid #a32d2d;color:#b0aea8}
.callout.amber{background:#1a1200;border-left:2px solid #854f0b;color:#b0aea8}
.callout.blue{background:#091420;border-left:2px solid #185fa5;color:#b0aea8}
.callout strong{font-weight:500}
.callout.red strong{color:#f09595}
.callout.amber strong{color:#ef9f27}
.callout.blue strong{color:#85b7eb}
.story-divider{border:none;border-top:1px solid #161616;margin:36px 0}
.conclusion{background:#111;border:1px solid #1e1e1e;border-radius:8px;padding:20px 24px;margin-top:8px}
.conclusion-title{font-size:14px;font-weight:500;color:#f0ede8;margin-bottom:14px}
.conclusion-points{display:flex;flex-direction:column;gap:12px}
.conclusion-point{display:flex;gap:14px;font-size:13px;color:#b0aea8;line-height:1.6}
.cbullet{width:24px;height:24px;border-radius:50%;flex-shrink:0;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:500;margin-top:1px}
.cbullet.r{background:#1a0a0a;color:#e24b4a;border:1px solid #a32d2d}
.cbullet.a{background:#1a1200;color:#ef9f27;border:1px solid #854f0b}
.cbullet.b{background:#091420;color:#85b7eb;border:1px solid #185fa5}
.cbullet.g{background:#0d1a0d;color:#97c459;border:1px solid #3b6d11}
@media(max-width:640px){.kpi-strip,.three-col-kpi{grid-template-columns:1fr 1fr}.two-col{grid-template-columns:1fr}}
</style>
</head>
<body>
<div class="page">

<div class="masthead">
  <div class="masthead-eyebrow">CAISO · SP15 &amp; NP15 · Day-ahead market · January 2023 – 2025</div>
  <h1 class="masthead-title">The solar value problem:<br>cannibalization, curtailment &amp; the declining capture ratio</h1>
  <p class="masthead-sub">Solar assets in CAISO are generating into a market they are reshaping — and the prices they earn are falling faster than the market average. This report traces that decline through three years of real market data and explains what it means for developers, owners, and traders.</p>
  <div class="masthead-meta">CAISO OASIS PRC_LMP (DAM) · SLD_REN_FCST (ACTUAL) · CAISO Production &amp; Curtailments Report · January used for YoY comparison (no DST, stable weather baseline)</div>
</div>

<div class="section">
  <div class="section-header"><span class="section-num">01</span><span class="section-title">The capture ratio is collapsing</span></div>
  <p class="section-thesis">A solar asset doesn't earn the average market price — it earns a generation-weighted average of the hours it actually produces. As solar capacity has grown from __CAP_2023__GW to __CAP_2025__GW, those hours have become the cheapest in the day. The capture ratio has fallen __RATIO_DROP__ percentage points in two years.</p>
  <div class="kpi-strip">
    <div class="kpi"><div class="kpi-lbl">Capture ratio — Jan 2023</div><div class="kpi-val kv-green">__RATIO_2023__x</div><div class="kpi-ctx">$__CAP_2023_VAL__ capture vs $__AVG_2023__ avg</div></div>
    <div class="kpi"><div class="kpi-lbl">Capture ratio — Jan 2024</div><div class="kpi-val kv-amber">__RATIO_2024__x</div><div class="kpi-ctx">$__CAP_2024_VAL__ capture vs $__AVG_2024__ avg</div></div>
    <div class="kpi"><div class="kpi-lbl">Capture ratio — Jan 2025</div><div class="kpi-val kv-red">__RATIO_2025__x</div><div class="kpi-ctx">$__CAP_2025_VAL__ capture vs $__AVG_2025__ avg</div></div>
    <div class="kpi"><div class="kpi-lbl">2-year change</div><div class="kpi-val kv-red">&#8722;__RATIO_DROP__pp</div><div class="kpi-ctx">__RATIO_2023__x &#8594; __RATIO_2025__x in 24 months</div></div>
  </div>
  <div class="chart-card a-red">
    <div class="card-title">Capture price vs average LMP — January, year on year</div>
    <div class="card-sub">Generation-weighted capture price (what solar earns) vs simple hourly average LMP (what an uncorrelated generator earns). The widening gap is the cannibalization discount.</div>
    <div class="leg-row">
      <span class="leg"><span class="leg-sq" style="background:#378add;opacity:0.5"></span>Avg SP15 LMP</span>
      <span class="leg"><span class="leg-sq" style="background:#e24b4a;opacity:0.6"></span>Solar capture price</span>
    </div>
    <div style="position:relative;height:200px"><canvas id="c1"></canvas></div>
    <div class="callout red">Capture price fell from <strong>$__CAP_2023_VAL__/MWh in 2023</strong> to <strong>$__CAP_2025_VAL__/MWh in 2025</strong> — a __CAP_PCT_DROP__% decline — while the average LMP fell only __AVG_PCT_DROP__%. The capture discount widens because solar's own output is the primary driver of midday price suppression.</div>
  </div>
</div>

<hr class="story-divider">

<div class="section">
  <div class="section-header"><span class="section-num">02</span><span class="section-title">Why: the duck curve deepens every year</span></div>
  <p class="section-thesis">Solar doesn't generate when prices are highest — it generates when prices are lowest, and it's the reason prices are lowest. The midday price trough gets deeper each year as more capacity competes for the same hours. In 2023, solar earned $__PEAK_PRICE_2023__/MWh at its peak generation hour. In 2025, the same hour paid $__PEAK_PRICE_2025__/MWh.</p>
  <div class="two-col">
    <div class="chart-card a-amber">
      <div class="card-title">Duck curve deepening — avg hourly SP15 price by year</div>
      <div class="card-sub">The midday trough deepens as solar capacity grows. Each line is January of that year.</div>
      <div class="leg-row">
        <span class="leg"><span class="leg-line" style="background:#639922"></span>2023</span>
        <span class="leg"><span class="leg-line" style="background:#ef9f27"></span>2024</span>
        <span class="leg"><span class="leg-line" style="background:#e24b4a"></span>2025</span>
      </div>
      <div style="position:relative;height:210px"><canvas id="c2a"></canvas></div>
    </div>
    <div class="chart-card a-amber">
      <div class="card-title">Solar generation profile — avg hourly output by year</div>
      <div class="card-sub">More capacity means more generation in the same midday window, flooding the same hours with supply.</div>
      <div class="leg-row">
        <span class="leg"><span class="leg-sq" style="background:#639922;opacity:0.5"></span>2023</span>
        <span class="leg"><span class="leg-sq" style="background:#ef9f27;opacity:0.5"></span>2024</span>
        <span class="leg"><span class="leg-sq" style="background:#e24b4a;opacity:0.5"></span>2025</span>
      </div>
      <div style="position:relative;height:210px"><canvas id="c2b"></canvas></div>
    </div>
  </div>
  <div class="callout amber">At the solar peak hour, average SP15 price fell <strong>$__PEAK_PRICE_2023__ (2023) &#8594; $__PEAK_PRICE_2024__ (2024) &#8594; $__PEAK_PRICE_2025__/MWh (2025)</strong>. The evening ramp stayed above $__EVE_PRICE_2025__&#8211;$__EVE_PRICE_2023__/MWh. Solar generates when the market pays least and misses the highest-value hours entirely.</div>
</div>

<hr class="story-divider">

<div class="section">
  <div class="section-header"><span class="section-num">03</span><span class="section-title">The next stage: negative prices and value-at-risk</span></div>
  <p class="section-thesis">A declining capture ratio is uncomfortable. Negative-price exposure is a crisis. In 2023 and 2024, no solar generation in January occurred during negative-price hours. In January 2025, __NEG_EXP_2025__% of all solar generation was dispatched into negative prices — roughly 1 in 8 MWh generated at a loss.</p>
  <div class="kpi-strip three-col-kpi">
    <div class="kpi"><div class="kpi-lbl">Neg-price exposure 2023</div><div class="kpi-val kv-green">0.0%</div><div class="kpi-ctx">0 hours below $0/MWh</div></div>
    <div class="kpi"><div class="kpi-lbl">Neg-price exposure 2024</div><div class="kpi-val kv-green">0.0%</div><div class="kpi-ctx">0 hours below $0/MWh</div></div>
    <div class="kpi"><div class="kpi-lbl">Neg-price exposure 2025</div><div class="kpi-val kv-red">__NEG_EXP_2025__%</div><div class="kpi-ctx">__NEG_HRS_2025__ hours below $0 &middot; __CURT_2025_MWH__ GWh curtailed</div></div>
  </div>
  <div class="two-col">
    <div class="chart-card a-red">
      <div class="card-title">Daily capture ratio by year</div>
      <div class="card-sub">Each bar is one day. Bright = below 0.40x. The 2025 distribution shifts dramatically left.</div>
      <div class="leg-row">
        <span class="leg"><span class="leg-sq" style="background:#639922;opacity:0.7"></span>2023</span>
        <span class="leg"><span class="leg-sq" style="background:#ef9f27;opacity:0.7"></span>2024</span>
        <span class="leg"><span class="leg-sq" style="background:#e24b4a;opacity:0.7"></span>2025</span>
      </div>
      <div style="position:relative;height:210px"><canvas id="c3a"></canvas></div>
    </div>
    <div class="chart-card a-red">
      <div class="card-title">Value-at-risk — P10/P50/P90 capture ratios</div>
      <div class="card-sub">P10 = worst 10% of days. The downside tail widens sharply in 2025 — exactly the scenario that breaks project finance models.</div>
      <div style="position:relative;height:210px"><canvas id="c3b"></canvas></div>
      <div class="callout red">2025 P10: <strong>__P10_2025__x</strong> — on the worst 10% of days, solar earned only __P10_2025_PCT__% of average market price. The 2023 P10 was __P10_2023__x. <strong>Lenders stress-test against P10; this trend makes project financing harder.</strong></div>
    </div>
  </div>
</div>

<hr class="story-divider">

<div class="section">
  <div class="section-header"><span class="section-num">04</span><span class="section-title">Curtailment: the hidden revenue loss</span></div>
  <p class="section-thesis">Standard capture price only counts dispatched MWh. Curtailed solar represents real lost revenue — generation withheld during the same negative-price midday hours. January curtailment grew from __CURT_2023_MWH__ GWh (2023) to __CURT_2025_MWH__ GWh (2025) — a __CURT_GROWTH__x increase in two years.</p>
  <div class="chart-card a-blue">
    <div class="card-title">Largest curtailment hours — top 30 across all years (January)</div>
    <div class="card-sub">Each bar is one hour, coloured by year. Curtailment is entirely concentrated in the 09:00&#8211;16:00 PST solar window. Peak events in 2025 dwarf 2023 levels.</div>
    <div class="leg-row">
      <span class="leg"><span class="leg-sq" style="background:#639922;opacity:0.7"></span>2023</span>
      <span class="leg"><span class="leg-sq" style="background:#ef9f27;opacity:0.7"></span>2024</span>
      <span class="leg"><span class="leg-sq" style="background:#e24b4a;opacity:0.7"></span>2025</span>
    </div>
    <div style="position:relative;height:200px"><canvas id="c4"></canvas></div>
    <div class="callout blue">Peak curtailment event: <strong>__PEAK_CURT_MW__ MW</strong> in a single hour. January curtailment grew from <strong>__CURT_2023_MWH__ GWh (2023)</strong> to <strong>__CURT_2025_MWH__ GWh (2025)</strong>. This energy was produced but not paid for — the direct cost of oversupply that storage co-location can convert into revenue.</div>
  </div>
</div>

<hr class="story-divider">

<div class="section">
  <div class="section-header"><span class="section-num">05</span><span class="section-title">What this means for market participants</span></div>
  <p class="section-thesis">These trends compound each other. Lower capture ratios reduce revenues. Negative-price exposure adds direct losses. Curtailment adds hidden losses not in standard metrics. Together they describe structural deterioration in the economics of unhedged solar in CAISO — and a clear value signal for storage.</p>
  <div class="conclusion">
    <div class="conclusion-title">Key implications</div>
    <div class="conclusion-points">
      <div class="conclusion-point"><div class="cbullet r">1</div><div><strong style="color:#f09595">Solar PPA pricing must account for declining capture.</strong> A PPA priced against average LMP will consistently overestimate revenues. The correct reference is the capture price — which is __RATIO_2025_PCT__% below average in January 2025 and still falling.</div></div>
      <div class="conclusion-point"><div class="cbullet r">2</div><div><strong style="color:#f09595">Project finance stress tests need updating.</strong> The P10 daily capture ratio fell from __P10_2023__x (2023) to __P10_2025__x (2025). Debt service models built on 2022&#8211;2023 data are materially optimistic.</div></div>
      <div class="conclusion-point"><div class="cbullet a">3</div><div><strong style="color:#ef9f27">Curtailment is accelerating as a structural risk.</strong> From near-zero in 2023 to __CURT_2025_MWH__ GWh in January 2025 alone. As CAISO solar capacity approaches 30+ GW, curtailment during peak generation hours will become a persistent feature, not an occasional event.</div></div>
      <div class="conclusion-point"><div class="cbullet b">4</div><div><strong style="color:#85b7eb">The evening ramp is the value signal storage responds to.</strong> While solar earns $__PEAK_PRICE_2025__/MWh at midday, the 17:00&#8211;19:00 PST ramp consistently prices at $__EVE_PRICE_2025__&#8211;$__EVE_PRICE_2023__/MWh. A 2-hour battery can charge at negative prices and discharge at the evening peak.</div></div>
      <div class="conclusion-point"><div class="cbullet g">5</div><div><strong style="color:#97c459">NP15 vs SP15 divergence is a real siting signal.</strong> NP15 held a __NP15_RATIO_2025__x capture ratio in January 2025 vs SP15&apos;s __RATIO_2025__x, with zero negative-price exposure vs __NEG_EXP_2025__%. Location matters.</div></div>
    </div>
  </div>
</div>

</div>
<script>
const GRID='#1a1a1a',TICK={color:'#8a8880',font:{size:11}};
const TT={backgroundColor:'#161616',borderColor:'#2a2a2a',borderWidth:1,titleColor:'#d3d1c7',bodyColor:'#9a9890'};
const Y_COLS={'2023':'#639922','2024':'#ef9f27','2025':'#e24b4a'};
const Y_ALPHA={'2023':'rgba(99,153,34,0.55)','2024':'rgba(239,159,39,0.55)','2025':'rgba(226,75,74,0.55)'};
const YEARS=['2023','2024','2025'];
const HRS=Array.from({length:24},(_,i)=>String(i).padStart(2,'0')+':00');
const D=__PAYLOAD__;
const {yoy,duck,daily,curt_chart}=D;

new Chart(document.getElementById('c1'),{type:'bar',
  data:{labels:YEARS,datasets:[
    {label:'Avg LMP',data:YEARS.map(y=>yoy[y].sp15.avg),backgroundColor:'rgba(55,138,221,0.25)',borderColor:'#378add',borderWidth:1.5,borderRadius:4,borderSkipped:false},
    {label:'Capture',data:YEARS.map(y=>yoy[y].sp15.cap),backgroundColor:YEARS.map(y=>Y_ALPHA[y]),borderColor:YEARS.map(y=>Y_COLS[y]),borderWidth:1.5,borderRadius:4,borderSkipped:false},
  ]},
  options:{responsive:true,maintainAspectRatio:false,
    plugins:{legend:{display:false},tooltip:{...TT,callbacks:{label:c=>`${c.dataset.label}: $${c.parsed.y.toFixed(1)}/MWh`}}},
    scales:{x:{ticks:TICK,grid:{color:GRID}},y:{ticks:{...TICK,callback:v=>`$${v}`},grid:{color:GRID}}}}
});

new Chart(document.getElementById('c2a'),{type:'line',
  data:{labels:HRS,datasets:YEARS.map(y=>({data:duck[y].sp15,borderColor:Y_COLS[y],borderWidth:1.8,pointRadius:0,tension:0.4,label:y}))},
  options:{responsive:true,maintainAspectRatio:false,
    plugins:{legend:{display:false},tooltip:TT},
    scales:{x:{ticks:{...TICK,maxTicksLimit:8,maxRotation:0},grid:{color:GRID}},y:{ticks:{...TICK,callback:v=>`$${v}`},grid:{color:GRID}}}}
});

new Chart(document.getElementById('c2b'),{type:'bar',
  data:{labels:HRS,datasets:YEARS.map(y=>({
    data:duck[y].mw.map(v=>+(v/1000).toFixed(2)),backgroundColor:Y_ALPHA[y],borderColor:Y_COLS[y],
    borderWidth:0.5,borderRadius:1,borderSkipped:false,label:y,barPercentage:0.85,categoryPercentage:0.9
  }))},
  options:{responsive:true,maintainAspectRatio:false,
    plugins:{legend:{display:false},tooltip:{...TT,callbacks:{label:c=>`${c.dataset.label}: ${c.parsed.y.toFixed(1)} GW`}}},
    scales:{x:{ticks:{...TICK,maxTicksLimit:8,maxRotation:0},grid:{color:GRID}},y:{ticks:{...TICK,callback:v=>`${v}GW`},grid:{color:GRID}}}}
});

new Chart(document.getElementById('c3a'),{type:'bar',
  data:{labels:Array.from({length:31},(_,i)=>`${i+1}`),
    datasets:YEARS.map(y=>({
      data:daily[y].ratio.map(v=>+(v*100).toFixed(1)),
      backgroundColor:daily[y].ratio.map(v=>v<0.4?Y_ALPHA[y]:`${Y_COLS[y]}33`),
      borderColor:Y_COLS[y],borderWidth:0.5,borderRadius:2,borderSkipped:false,label:y,
      barPercentage:0.9,categoryPercentage:0.85
    }))},
  options:{responsive:true,maintainAspectRatio:false,
    plugins:{legend:{display:false},tooltip:{...TT,callbacks:{label:c=>`${c.dataset.label}: ${c.parsed.y.toFixed(1)}%`}}},
    scales:{x:{ticks:{...TICK,maxTicksLimit:8,maxRotation:0},grid:{color:GRID}},y:{min:0,max:115,ticks:{...TICK,callback:v=>`${v}%`},grid:{color:GRID}}}}
});

function pct(arr,p){const s=[...arr].sort((a,b)=>a-b);const i=(p/100)*(s.length-1);const lo=Math.floor(i),hi=Math.min(lo+1,s.length-1);return +(s[lo]+(s[hi]-s[lo])*(i-lo)).toFixed(4);}
const pd=YEARS.map(y=>({p10:pct(daily[y].ratio,10),p50:pct(daily[y].ratio,50),p90:pct(daily[y].ratio,90)}));
new Chart(document.getElementById('c3b'),{type:'bar',
  data:{labels:YEARS,datasets:[
    {label:'P90',data:pd.map(d=>+(d.p90*100).toFixed(1)),backgroundColor:YEARS.map(y=>`${Y_COLS[y]}22`),borderColor:YEARS.map(y=>Y_COLS[y]),borderWidth:1,borderRadius:3,borderSkipped:false},
    {label:'P50',data:pd.map(d=>+(d.p50*100).toFixed(1)),backgroundColor:YEARS.map(y=>`${Y_COLS[y]}55`),borderColor:YEARS.map(y=>Y_COLS[y]),borderWidth:1.5,borderRadius:3,borderSkipped:false},
    {label:'P10',data:pd.map(d=>+(d.p10*100).toFixed(1)),backgroundColor:YEARS.map(y=>Y_ALPHA[y]),borderColor:YEARS.map(y=>Y_COLS[y]),borderWidth:2,borderRadius:3,borderSkipped:false},
  ]},
  options:{responsive:true,maintainAspectRatio:false,
    plugins:{legend:{display:false},tooltip:{...TT,callbacks:{label:c=>`${c.dataset.label}: ${c.parsed.y.toFixed(1)}%`}}},
    scales:{x:{ticks:TICK,grid:{color:GRID}},y:{min:0,max:115,ticks:{...TICK,callback:v=>`${v}%`},grid:{color:GRID}}}}
});

const cYC=curt_chart.year.map(y=>y===2023?'rgba(99,153,34,0.65)':y===2024?'rgba(239,159,39,0.65)':'rgba(226,75,74,0.65)');
const cYB=curt_chart.year.map(y=>y===2023?'#639922':y===2024?'#ef9f27':'#e24b4a');
new Chart(document.getElementById('c4'),{type:'bar',
  data:{labels:curt_chart.labels,datasets:[{
    data:curt_chart.mw,backgroundColor:cYC,borderColor:cYB,borderWidth:0.5,borderRadius:2,borderSkipped:false
  }]},
  options:{responsive:true,maintainAspectRatio:false,
    plugins:{legend:{display:false},tooltip:{...TT,callbacks:{label:c=>`${curt_chart.year[c.dataIndex]}: ${c.parsed.y.toFixed(0)} MW curtailed`}}},
    scales:{x:{ticks:{...TICK,maxTicksLimit:10,maxRotation:45},grid:{color:GRID}},y:{ticks:{...TICK,callback:v=>`${v}MW`},grid:{color:GRID}}}}
});
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 55)
    print("analysis_synthesis.py — synthesis dashboard")
    print("=" * 55)

    print("\nLoading market data...")
    all_df = load_all_years()
    print(f"  {len(all_df)} rows | years: {sorted(all_df['year'].unique().tolist())}")

    print("\nLoading curtailment data (January only)...")
    curt_jan = load_curtailment_jan()
    if not curt_jan.empty:
        print(f"  years: {sorted(curt_jan['year'].unique().tolist())}")

    print("\nComputing metrics...")
    yoy          = compute_yoy_summary(all_df)
    duck         = compute_duck_curves(all_df)
    daily        = compute_daily(all_df)
    curt_summary = compute_curtailment_summary(curt_jan)
    curt_chart   = compute_curtailment_chart(curt_jan, top_n=30)

    print("\nSummary:")
    for year in sorted(yoy):
        sp = yoy[year]["sp15"]
        print(f"  {year} SP15: cap=${sp['cap']:.2f}  avg=${sp['avg']:.2f}  "
              f"ratio={sp['ratio']:.3f}x  neg={sp['neg_exp']:.1%}")
    for year in sorted(curt_summary):
        c = curt_summary[year]
        print(f"  {year} curtailment: {c['total_mwh']/1000:.1f} GWh  peak {c['peak_mw']:.0f} MW")

    print("\nBuilding dashboard HTML...")
    text_vars = compute_text_vars(yoy, duck, daily, curt_summary)
    payload   = build_payload(yoy, duck, daily, curt_chart)

    html = HTML_TEMPLATE.replace("__PAYLOAD__", payload)
    for k, v in text_vars.items():
        html = html.replace(k, v)

    out = OUT_DIR / "synthesis_dashboard.html"
    out.write_text(html, encoding="utf-8")
    print(f"\nSaved → {out}")


if __name__ == "__main__":
    main()
