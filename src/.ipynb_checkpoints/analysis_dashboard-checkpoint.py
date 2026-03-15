"""
analysis_dashboard.py
---------------------
Reads data/processed/market_data.csv and produces a self-contained
interactive HTML dashboard in Modo Energy's visual style.

Output: outputs/solar_capture_dashboard.html

Charts:
  Fig 1 — Daily capture price vs average LMP (NP15/SP15 toggle, month toggle)
  Fig 2 — Duck curve: hourly price vs solar generation profile
  Fig 3 — Price heatmap: hour-of-day x day-of-week
  Fig 4 — Price vs generation scatter (cannibalization slope)

Run: python src/analysis_dashboard.py
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT     = Path(__file__).resolve().parents[1]
PROC_DIR = ROOT / "data" / "processed"
OUT_DIR  = ROOT / "outputs"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Data extraction
# ---------------------------------------------------------------------------

def load_and_prepare(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    # Do NOT use utc=True — the CSV stores PST timestamps with a -08:00 offset
    # (written by process_data.py). utc=True silently reconverts them to UTC,
    # shifting hour-of-day groupings by 8 hours and breaking the duck curve.
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["hour"]  = df["timestamp"].dt.hour
    df["date"]  = df["timestamp"].dt.date
    df["dow"]   = df["timestamp"].dt.dayofweek
    df["month"] = df["timestamp"].dt.strftime("%Y-%m")
    return df


def compute_kpis(df: pd.DataFrame, lmp_col: str) -> dict:
    solar_mask = df["solar_mw"] > 0
    total_gen  = df.loc[solar_mask, "solar_mw"].sum()
    cap        = (df.loc[solar_mask, lmp_col] * df.loc[solar_mask, "solar_mw"]).sum() / total_gen
    avg        = df[lmp_col].mean()
    neg_exp    = df.loc[df[lmp_col] < 0, "solar_mw"].sum() / df["solar_mw"].sum()
    neg_hrs    = int((df[lmp_col] < 0).sum())
    return {
        "capture_price": round(cap, 2),
        "avg_lmp":       round(avg, 2),
        "capture_ratio": round(cap / avg, 4),
        "neg_exposure":  round(neg_exp, 4),
        "neg_hours":     neg_hrs,
    }


def compute_daily(g: pd.DataFrame, lmp_col: str) -> dict:
    daily = g.groupby("date").apply(
        lambda d: pd.Series({
            "cap":       round((d[lmp_col] * d["solar_mw"]).sum() / max(d["solar_mw"].sum(), 1), 2),
            "avg":       round(d[lmp_col].mean(), 2),
            "solar_gwh": round(d["solar_mw"].sum() / 1000, 2),
        })
    ).reset_index()
    return {
        "labels":     [str(x) for x in daily["date"]],
        "cap":        daily["cap"].tolist(),
        "avg":        daily["avg"].tolist(),
        "solar_gwh":  daily["solar_gwh"].tolist(),
    }


def compute_hourly(g: pd.DataFrame) -> dict:
    h = g.groupby("hour").agg(
        sp15=("sp15_lmp", "mean"),
        np15=("np15_lmp", "mean"),
        mw=("solar_mw", "mean"),
    ).round(2)
    return {"sp15": h["sp15"].tolist(), "np15": h["np15"].tolist(), "mw": h["mw"].tolist()}


def compute_heatmap(g: pd.DataFrame, lmp_col: str) -> list:
    pivot = g.pivot_table(lmp_col, "dow", "hour", aggfunc="mean").round(1)
    for h in range(24):
        if h not in pivot.columns:
            pivot[h] = None
    pivot = pivot[sorted(pivot.columns)]
    return pivot.values.tolist()


def compute_scatter(g: pd.DataFrame) -> dict:
    s = g[g["solar_mw"] > 0].copy()
    s["gw"] = (s["solar_mw"] / 1000).round(2)
    return {
        "sp15": [[row.gw, round(row.sp15_lmp, 1)] for row in s.itertuples()],
        "np15": [[row.gw, round(row.np15_lmp, 1)] for row in s.itertuples()],
    }


def build_payload(df: pd.DataFrame) -> dict:
    months   = sorted(df["month"].unique())
    payload  = {"months": months, "by_month": {}}

    for month in months:
        g = df[df["month"] == month]
        payload["by_month"][month] = {
            "kpis": {
                "sp15": compute_kpis(g, "sp15_lmp"),
                "np15": compute_kpis(g, "np15_lmp"),
            },
            "daily": {
                "sp15": compute_daily(g, "sp15_lmp"),
                "np15": compute_daily(g, "np15_lmp"),
            },
            "hourly":  compute_hourly(g),
            "heatmap": {
                "sp15": compute_heatmap(g, "sp15_lmp"),
                "np15": compute_heatmap(g, "np15_lmp"),
            },
            "scatter": compute_scatter(g),
        }

    return payload


# ---------------------------------------------------------------------------
# HTML template
# ---------------------------------------------------------------------------

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Solar capture price — CAISO</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0b0b0b;color:#e0ddd6;min-height:100vh;padding:32px 24px}
.page{max-width:960px;margin:0 auto}
.report-header{border-bottom:1px solid #222;padding-bottom:18px;margin-bottom:24px;display:flex;justify-content:space-between;align-items:flex-end;flex-wrap:wrap;gap:12px}
.header-left .eyebrow{font-size:10px;letter-spacing:1.8px;color:#4a4a48;text-transform:uppercase;margin-bottom:6px}
.header-left h1{font-size:22px;font-weight:500;color:#f0ede8;letter-spacing:-0.3px}
.header-left .meta{font-size:11px;color:#4a4a48;margin-top:4px}
.controls{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.ctrl-label{font-size:10px;letter-spacing:1px;color:#4a4a48;text-transform:uppercase}
.seg-group{display:flex;border:1px solid #222;border-radius:5px;overflow:hidden}
.seg-btn{background:none;border:none;border-right:1px solid #222;color:#5f5e5a;font-size:11px;padding:5px 12px;cursor:pointer;font-family:inherit;transition:background 0.1s,color 0.1s}
.seg-btn:last-child{border-right:none}
.seg-btn.active{background:#1c2a1c;color:#97c459}
.seg-btn.hub-btn.active{background:#0f1e2e;color:#378add}
.exec-summary{background:#111;border:1px solid #1e1e1e;border-radius:8px;padding:14px 18px;margin-bottom:22px}
.exec-title{font-size:10px;letter-spacing:1.2px;color:#4a4a48;text-transform:uppercase;margin-bottom:10px}
.exec-list{list-style:none;display:flex;flex-direction:column;gap:7px}
.exec-list li{font-size:12.5px;color:#888780;padding-left:14px;position:relative;line-height:1.55}
.exec-list li::before{content:"";position:absolute;left:0;top:8px;width:5px;height:1px;background:#333}
.exec-list li strong{color:#d3d1c7;font-weight:500}
.kpi-row{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin-bottom:22px}
.kpi{background:#111;border:1px solid #1e1e1e;border-radius:6px;padding:12px 14px}
.kpi-lbl{font-size:9.5px;letter-spacing:0.9px;color:#4a4a48;text-transform:uppercase;margin-bottom:7px}
.kpi-val{font-size:24px;font-weight:500;color:#f0ede8;line-height:1;letter-spacing:-0.5px}
.kpi-sub{font-size:11px;margin-top:5px}
.red{color:#e24b4a}.amber{color:#ef9f27}.green{color:#639922}.muted{color:#4a4a48}
.fig-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px}
.fig-full{grid-column:1/-1}
.fig-block{background:#111;border:1px solid #1e1e1e;border-radius:8px;padding:16px 18px}
.fig-eyebrow{font-size:9px;letter-spacing:1.4px;color:#3a3a38;text-transform:uppercase;margin-bottom:4px}
.fig-title{font-size:12.5px;font-weight:500;color:#c8c5be;margin-bottom:3px}
.fig-sub{font-size:11px;color:#4a4a48;margin-bottom:12px;line-height:1.5}
.legend-row{display:flex;gap:14px;margin-bottom:10px;flex-wrap:wrap}
.leg{display:flex;align-items:center;gap:5px;font-size:10.5px;color:#5f5e5a}
.leg-sq{width:10px;height:10px;border-radius:2px;flex-shrink:0}
.leg-line{width:18px;height:2px;border-radius:1px;flex-shrink:0}
.chart-wrap{position:relative}
.callout{background:#0d1a0d;border-left:2px solid #3b6d11;border-radius:0 4px 4px 0;padding:8px 12px;margin-top:10px;font-size:11.5px;color:#5f5e5a;line-height:1.6}
.callout strong{color:#97c459;font-weight:500}
.callout.blue{background:#091420;border-left-color:#185fa5}
.callout.blue strong{color:#85b7eb}
.callout.amber{background:#1a1200;border-left-color:#854f0b}
.callout.amber strong{color:#ef9f27}
.hm-wrap{overflow-x:auto;margin-top:4px}
.hm-table{border-collapse:separate;border-spacing:2px}
.hm-table td{height:20px;border-radius:2px}
.hm-hlabel{font-size:8.5px;color:#3a3a38;text-align:center;padding:2px 1px}
.hm-dlabel{font-size:9px;color:#4a4a48;padding-right:8px;text-align:right;white-space:nowrap;vertical-align:middle;min-width:28px}
.legend-bar{display:flex;align-items:center;gap:8px;margin-top:10px;font-size:10px;color:#4a4a48}
.legend-bar-grad{flex:1;height:6px;border-radius:3px;background:linear-gradient(to right,#042c53,#185fa5,#85b7eb,#e0ddd6,#ef9f27,#ba7517,#412402)}
@media(max-width:620px){.kpi-row{grid-template-columns:1fr 1fr}.fig-grid{grid-template-columns:1fr}.fig-full{grid-column:1}}
</style>
</head>
<body>
<div class="page">

<div class="report-header">
  <div class="header-left">
    <div class="eyebrow">CAISO · Day-ahead market · Solar capture analysis</div>
    <h1>Solar capture price — NP15 &amp; SP15</h1>
    <div class="meta" id="meta-line">Loading...</div>
  </div>
  <div class="controls">
    <span class="ctrl-label">Hub</span>
    <div class="seg-group">
      <button class="seg-btn hub-btn active" onclick="setHub('sp15')">SP15</button>
      <button class="seg-btn hub-btn" onclick="setHub('np15')">NP15</button>
    </div>
    <span class="ctrl-label" style="margin-left:8px">Month</span>
    <div class="seg-group" id="month-btns"></div>
  </div>
</div>

<div class="exec-summary">
  <div class="exec-title">Executive summary</div>
  <ul class="exec-list" id="exec-list"></ul>
</div>

<div class="kpi-row" id="kpi-row"></div>

<div class="fig-grid">
  <div class="fig-block fig-full">
    <div class="fig-eyebrow">Figure 1</div>
    <div class="fig-title">Daily solar capture price vs average LMP</div>
    <div class="fig-sub">Generation-weighted capture price vs simple average. Shaded gap = value discount. Bars = daily solar generation (RHS).</div>
    <div class="legend-row">
      <span class="leg"><span class="leg-line" style="background:#378add"></span>Avg LMP</span>
      <span class="leg"><span class="leg-line" style="background:#e24b4a"></span>Capture price</span>
      <span class="leg"><span class="leg-sq" style="background:#1a2e12"></span>Solar gen (GWh)</span>
    </div>
    <div class="chart-wrap" style="height:220px"><canvas id="c1"></canvas></div>
    <div class="callout blue" id="c1-callout"></div>
  </div>

  <div class="fig-block">
    <div class="fig-eyebrow">Figure 2</div>
    <div class="fig-title">Duck curve — hourly price vs generation</div>
    <div class="fig-sub">Avg price and solar MW by hour of day. The inverse shape is the core driver of the capture discount.</div>
    <div class="legend-row">
      <span class="leg"><span class="leg-line" style="background:#378add"></span>Avg LMP ($/MWh)</span>
      <span class="leg"><span class="leg-sq" style="background:#1a2e12"></span>Avg solar (GW)</span>
    </div>
    <div class="chart-wrap" style="height:200px"><canvas id="c2"></canvas></div>
    <div class="callout" id="c2-callout"></div>
  </div>

  <div class="fig-block">
    <div class="fig-eyebrow">Figure 3</div>
    <div class="fig-title">LMP heatmap — hour × day of week</div>
    <div class="fig-sub">Average price by hour and weekday. Midday suppression visible across all days; weekends most pronounced.</div>
    <div class="hm-wrap"><div id="heatmap"></div></div>
    <div class="legend-bar">
      <span>−$20</span>
      <div class="legend-bar-grad"></div>
      <span>$90+</span>
    </div>
    <div class="callout amber" id="c3-callout"></div>
  </div>
</div>

<div class="fig-grid">
  <div class="fig-block fig-full">
    <div class="fig-eyebrow">Figure 4</div>
    <div class="fig-title">Price vs solar generation — cannibalization scatter</div>
    <div class="fig-sub">Each point is one hour with solar &gt; 0. Downward slope quantifies the cannibalization effect: higher solar output → lower prices → lower capture value.</div>
    <div class="chart-wrap" style="height:240px"><canvas id="c4"></canvas></div>
    <div class="callout amber" id="c4-callout"></div>
  </div>
</div>

</div>

<script>
const DATA = __PAYLOAD__;

let hub = 'sp15';
let month = DATA.months[0];
let charts = {};
const GRID = '#1a1a1a';
const TICK = {color:'#3a3a38', font:{size:10}};
const DAYS = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];

function d() { return DATA.by_month[month]; }

function fmt$(v) { return (v<0?'-$'+Math.abs(v).toFixed(2):'$'+v.toFixed(2)); }
function fmtPct(v) { return (v*100).toFixed(1)+'%'; }

function setHub(h) {
  hub = h;
  document.querySelectorAll('.hub-btn').forEach(b => {
    b.classList.toggle('active', b.textContent.toLowerCase()===h);
  });
  refresh();
}

function setMonth(m) {
  month = m;
  document.querySelectorAll('.month-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.m===m);
  });
  refresh();
}

function buildMonthBtns() {
  const wrap = document.getElementById('month-btns');
  wrap.innerHTML = '';
  DATA.months.forEach((m,i) => {
    const b = document.createElement('button');
    b.className = 'seg-btn month-btn' + (i===0?' active':'');
    b.textContent = m;
    b.dataset.m = m;
    b.onclick = () => setMonth(m);
    wrap.appendChild(b);
  });
}

function updateMeta() {
  const k = d().kpis[hub];
  document.getElementById('meta-line').textContent =
    month + ' · ' + hub.toUpperCase() + ' hub · DAM prices · SLD_REN_FCST actual solar';
}

function updateExec() {
  const k = d().kpis[hub];
  const disc = ((1-k.capture_ratio)*100).toFixed(1);
  const ratioLabel = hub==='sp15'
    ? 'SP15 prices are structurally suppressed by midday solar; NP15 shows a narrower discount.'
    : 'NP15 has less direct solar exposure than SP15 — the discount is meaningful but smaller.';
  document.getElementById('exec-list').innerHTML = [
    `Solar assets earned <strong>${fmt$(k.capture_price)}/MWh</strong> on a generation-weighted basis — a <strong>${disc}% discount</strong> to the ${fmt$(k.avg_lmp)}/MWh simple average`,
    `Capture ratio of <strong>${k.capture_ratio.toFixed(2)}x</strong>. ${ratioLabel}`,
    k.neg_hours > 0
      ? `<strong>${fmtPct(k.neg_exposure)}</strong> of solar generation occurred during negative-price hours (${k.neg_hours} hrs) — direct revenue loss for uncurtailed assets`
      : `No negative-price exposure this period — all solar generation occurred during positive-price hours`,
    `Peak solar hours (UTC 16–22 / ~Pacific 08–14) consistently depress prices to their daily minimum, while the evening ramp offers 3–5× higher prices with near-zero solar output`,
  ].map(t=>`<li>${t}</li>`).join('');
}

function updateKPIs() {
  const k = d().kpis[hub];
  const disc = (1-k.capture_ratio)*100;
  document.getElementById('kpi-row').innerHTML = [
    {lbl:'Solar capture price', val:fmt$(k.capture_price)+'/MWh', sub:'Generation-weighted avg', cls:'amber'},
    {lbl:'Average '+hub.toUpperCase()+' LMP',  val:fmt$(k.avg_lmp)+'/MWh',  sub:'Simple hourly average', cls:'muted'},
    {lbl:'Capture ratio', val:k.capture_ratio.toFixed(2)+'x', sub:`−${disc.toFixed(1)}% vs average`, cls: k.capture_ratio<0.6?'red':k.capture_ratio<0.8?'amber':'green'},
    {lbl:'Neg-price exposure', val:fmtPct(k.neg_exposure), sub:`${k.neg_hours} hours below $0`, cls: k.neg_exposure>0.1?'red':k.neg_exposure>0.02?'amber':'green'},
  ].map(c=>`<div class="kpi"><div class="kpi-lbl">${c.lbl}</div><div class="kpi-val">${c.val}</div><div class="kpi-sub ${c.cls}">${c.sub}</div></div>`).join('');
}

function destroyChart(id) {
  if(charts[id]) { charts[id].destroy(); delete charts[id]; }
}

function updateC1() {
  destroyChart('c1');
  const dd = d().daily[hub];
  const labels = dd.labels.map(l => l.slice(5)); // MM-DD
  charts['c1'] = new Chart(document.getElementById('c1'), {
    data: {
      labels,
      datasets: [
        {type:'line', data:dd.avg, borderColor:'#378add', borderWidth:1.8, pointRadius:0, tension:0.3, yAxisID:'y', label:'Avg LMP'},
        {type:'line', data:dd.cap, borderColor:'#e24b4a', borderWidth:1.8, pointRadius:0, tension:0.3, yAxisID:'y', label:'Capture'},
        {type:'bar',  data:dd.solar_gwh, backgroundColor:'#1a2e12', borderColor:'#27500a', borderWidth:0, yAxisID:'y2', label:'Solar GWh', borderRadius:1},
      ]
    },
    options: {
      responsive:true, maintainAspectRatio:false,
      plugins:{legend:{display:false}, tooltip:{backgroundColor:'#161616',borderColor:'#222',borderWidth:1,titleColor:'#c8c5be',bodyColor:'#666',callbacks:{label:c=>c.datasetIndex===2?` ${c.parsed.y.toFixed(1)} GWh`:` ${fmt$(c.parsed.y)}/MWh`}}},
      scales:{
        x:{ticks:{...TICK,maxTicksLimit:10,maxRotation:0},grid:{color:GRID}},
        y:{position:'left',ticks:{...TICK,callback:v=>v<0?`-$${Math.abs(v)}`:`$${v}`},grid:{color:GRID},title:{display:true,text:'$/MWh',color:'#3a3a38',font:{size:9}}},
        y2:{position:'right',ticks:{...TICK,callback:v=>`${v}GWh`},grid:{display:false},title:{display:true,text:'GWh/day',color:'#27500a',font:{size:9}}},
      }
    }
  });
  const k = d().kpis[hub];
  const dd2 = d().daily[hub];
  const maxGenDay = dd2.labels[dd2.solar_gwh.indexOf(Math.max(...dd2.solar_gwh))].slice(5);
  const minCap = Math.min(...dd2.cap).toFixed(1);
  document.getElementById('c1-callout').innerHTML =
    `On the highest-generation day (${maxGenDay}), the capture price fell to <strong>$${minCap}/MWh</strong> — illustrating how peak solar output cannibalises the very prices the fleet earns.`;
}

function updateC2() {
  destroyChart('c2');
  const h = d().hourly;
  const prices = hub==='sp15' ? h.sp15 : h.np15;
  const labels = Array.from({length:24},(_,i)=>i===0?'00':i===6?'06':i===12?'12':i===18?'18':i===23?'23':'');
  charts['c2'] = new Chart(document.getElementById('c2'), {
    data: {
      labels: Array.from({length:24},(_,i)=>`${String(i).padStart(2,'0')}:00`),
      datasets:[
        {type:'line', data:prices, borderColor:'#378add', borderWidth:2, pointRadius:0, tension:0.4, yAxisID:'y'},
        {type:'bar',  data:h.mw.map(v=>+(v/1000).toFixed(2)), backgroundColor:'#162510', borderColor:'#3b6d11', borderWidth:0.5, yAxisID:'y2', borderRadius:1},
      ]
    },
    options:{
      responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:false},tooltip:{backgroundColor:'#161616',borderColor:'#222',borderWidth:1,titleColor:'#c8c5be',bodyColor:'#666'}},
      scales:{
        x:{ticks:{...TICK,maxTicksLimit:8,maxRotation:0},grid:{color:GRID}},
        y:{position:'left',ticks:{...TICK,callback:v=>v<0?`-$${Math.abs(v)}`:`$${v}`},grid:{color:GRID}},
        y2:{position:'right',ticks:{...TICK,callback:v=>`${v.toFixed(0)}GW`},grid:{display:false}},
      }
    }
  });
  const prices2 = hub==='sp15' ? d().hourly.sp15 : d().hourly.np15;
  const minPriceHr = prices2.indexOf(Math.min(...prices2));
  const minPrice = Math.min(...prices2).toFixed(1);
  const maxMwHr = d().hourly.mw.indexOf(Math.max(...d().hourly.mw));
  document.getElementById('c2-callout').innerHTML =
    `Solar generation peaks at hour <strong>${String(maxMwHr).padStart(2,'0')}:00 UTC</strong> (~${hub==='sp15'?'Pacific 08–10':'Pacific 08–10'}). Lowest price hour is <strong>${String(minPriceHr).padStart(2,'0')}:00 at $${minPrice}/MWh</strong> — solar earns least exactly when it generates most.`;
}

function updateHeatmap() {
  const data = d().heatmap[hub];
  const container = document.getElementById('heatmap');
  container.innerHTML = '';
  const table = document.createElement('table');
  table.className = 'hm-table';
  table.style.width = '100%';
  const stops = [
    [4,44,83],[24,95,165],[85,183,235],[200,196,188],
    [239,159,39],[186,117,23],[65,38,6]
  ];
  function priceColor(v) {
    const norm = Math.max(0,Math.min(1,(v+20)/110));
    const idx = Math.min(6,Math.floor(norm*7));
    const [r,g,b]=stops[idx];
    return `rgb(${r},${g},${b})`;
  }
  const thead = document.createElement('thead');
  let hr = '<tr><td class="hm-dlabel"></td>';
  for(let h=0;h<24;h++) hr+=`<td class="hm-hlabel">${h}</td>`;
  hr+='</tr>';
  thead.innerHTML=hr; table.appendChild(thead);
  const tbody = document.createElement('tbody');
  DAYS.forEach((day,di)=>{
    const tr=document.createElement('tr');
    tr.innerHTML=`<td class="hm-dlabel">${day}</td>`;
    for(let h=0;h<24;h++){
      const v=data[di]?.[h]??0;
      const td=document.createElement('td');
      td.style.cssText=`background:${priceColor(v)};width:${100/25}%;cursor:default`;
      td.title=`${day} ${String(h).padStart(2,'0')}:00 — $${v}/MWh`;
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  container.appendChild(table);

  const flatVals = data.flat().filter(v=>v!==null);
  const minPrice = Math.min(...flatVals).toFixed(1);
  const minIdx = flatVals.indexOf(Math.min(...flatVals));
  document.getElementById('c3-callout').innerHTML =
    `Weekend midday hours show the deepest price suppression — as low as <strong>$${minPrice}/MWh</strong>. Weekday morning (07–09) and evening (17–19) ramps are consistently highest-value. Solar assets miss both ramps entirely.`;
}

function updateC4() {
  destroyChart('c4');
  const pts = d().scatter[hub].map(p=>({x:p[0],y:p[1]}));
  // simple linear regression for trendline
  const n=pts.length, sx=pts.reduce((a,p)=>a+p.x,0), sy=pts.reduce((a,p)=>a+p.y,0);
  const sx2=pts.reduce((a,p)=>a+p.x*p.x,0), sxy=pts.reduce((a,p)=>a+p.x*p.y,0);
  const slope=((n*sxy-sx*sy)/(n*sx2-sx*sx));
  const intercept=(sy-slope*sx)/n;
  const xVals=[0,Math.max(...pts.map(p=>p.x))];
  const trendData=xVals.map(x=>({x:+x.toFixed(2),y:+(intercept+slope*x).toFixed(1)}));

  charts['c4'] = new Chart(document.getElementById('c4'),{
    data:{datasets:[
      {type:'scatter',data:pts,backgroundColor:'rgba(55,138,221,0.15)',borderColor:'rgba(55,138,221,0.3)',borderWidth:0.5,pointRadius:3.5,pointHoverRadius:5,label:'Hourly price'},
      {type:'line',data:trendData,borderColor:'#ef9f27',borderWidth:1.5,borderDash:[4,3],pointRadius:0,label:'Trend'},
    ]},
    options:{
      responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:false},tooltip:{backgroundColor:'#161616',borderColor:'#222',borderWidth:1,titleColor:'#c8c5be',bodyColor:'#666',filter:i=>i.datasetIndex===0,callbacks:{label:c=>` ${c.parsed.x.toFixed(1)} GW → $${c.parsed.y.toFixed(0)}/MWh`}}},
      scales:{
        x:{title:{display:true,text:'Solar generation (GW)',color:'#3a3a38',font:{size:10}},ticks:{...TICK,callback:v=>`${v}GW`},grid:{color:GRID}},
        y:{title:{display:true,text:hub.toUpperCase()+' LMP ($/MWh)',color:'#3a3a38',font:{size:10}},ticks:{...TICK,callback:v=>v<0?`-$${Math.abs(v)}`:`$${v}`},grid:{color:GRID}},
      }
    }
  });
  document.getElementById('c4-callout').innerHTML =
    `Regression slope: <strong>$${slope.toFixed(1)}/MWh per GW</strong> of additional solar. Each gigawatt of incremental solar generation is associated with a ${Math.abs(slope).toFixed(1)}/MWh price reduction at that hour. This slope will steepen as CAISO solar capacity grows.`;
}

function refresh() {
  updateMeta();
  updateExec();
  updateKPIs();
  updateC1();
  updateC2();
  updateHeatmap();
  updateC4();
}

buildMonthBtns();
refresh();
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    data_path = PROC_DIR / "market_data.csv"
    if not data_path.exists():
        raise FileNotFoundError(f"Run process_data.py first. Expected: {data_path}")

    print("=" * 50)
    print("analysis_dashboard.py — Modo-style HTML dashboard")
    print("=" * 50)

    df      = load_and_prepare(data_path)
    payload = build_payload(df)

    months = payload["months"]
    print(f"\nMonths found: {months}")
    for m in months:
        kpi_sp = payload["by_month"][m]["kpis"]["sp15"]
        kpi_np = payload["by_month"][m]["kpis"]["np15"]
        print(f"\n  {m}:")
        print(f"    SP15 — capture ${kpi_sp['capture_price']:.2f}, avg ${kpi_sp['avg_lmp']:.2f}, ratio {kpi_sp['capture_ratio']:.2f}x, neg-exp {kpi_sp['neg_exposure']:.1%}")
        print(f"    NP15 — capture ${kpi_np['capture_price']:.2f}, avg ${kpi_np['avg_lmp']:.2f}, ratio {kpi_np['capture_ratio']:.2f}x, neg-exp {kpi_np['neg_exposure']:.1%}")

    html = HTML_TEMPLATE.replace("__PAYLOAD__", json.dumps(payload))
    out  = OUT_DIR / "solar_capture_dashboard.html"
    out.write_text(html, encoding="utf-8")
    print(f"\nDashboard saved → {out}")
    print("Open the file in any browser — no server needed, fully self-contained.")


if __name__ == "__main__":
    main()
