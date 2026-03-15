# Solar Capture Price — CAISO

Analysis of solar cannibalization and the declining capture ratio in the California ISO (CAISO) electricity market, using three years of Day-Ahead Market data (January 2023–2025).

---

## Problem statement

Solar assets in CAISO don't earn the average market price — they earn a **generation-weighted average of the hours they produce**. Because solar generates heavily during midday, and because solar's own output is the primary driver of midday price suppression, the prices solar earns have been falling faster than the market average. This effect — known as **solar cannibalization** — means that as California installs more solar capacity, each incremental GW earns less per MWh than the last.

This project quantifies that decline across three dimensions:

1. **Capture ratio** — how much less than average does solar actually earn?
2. **Value-at-risk** — how bad are the worst days, and is the downside tail widening?
3. **Curtailment** — beyond the price discount, how much potential generation is simply being withheld during oversupply events?

The findings matter directly for PPA pricing, project finance underwriting, and the investment case for storage co-location. A solar project modelled against average LMP will overestimate revenues. A project financed against 2022–2023 capture ratios faces material underperformance in 2025.

---

## Key findings (January 2023–2025)

| Metric | Jan 2023 | Jan 2024 | Jan 2025 |
|--------|----------|----------|----------|
| SP15 capture price | $97.49/MWh | $36.99/MWh | $17.43/MWh |
| SP15 average LMP | $138.72/MWh | $65.57/MWh | $41.35/MWh |
| SP15 capture ratio | 0.70x | 0.56x | 0.42x |
| NP15 capture ratio | 0.82x | 0.92x | 0.72x |
| Negative-price exposure | 0.0% | 0.0% | 12.7% |
| January curtailment | ~50 GWh | ~96 GWh | ~115 GWh |
| Installed solar capacity | ~16.2 GW | ~19.8 GW | ~23.5 GW |

The SP15 capture ratio fell **28 percentage points** in two years. Negative-price exposure — zero in both 2023 and 2024 — jumped to 12.7% of total January solar generation in 2025. Curtailment more than doubled over the same period.

---

## Dashboards

The project produces five self-contained HTML dashboards, each addressing a different analytical lens.

### `synthesis_dashboard.html` — The main narrative

The synthesis dashboard tells the complete story in five sections, designed to be shared directly with developers, lenders, and traders.

**Section 01 — The capture ratio is collapsing**
A grouped bar chart shows capture price vs average LMP for each January (2023/2024/2025). The bars make the widening gap between what solar earns and what the market pays immediately visible. KPI cards anchor the headline numbers. The key insight: capture price fell 82% over two years while average LMP fell only 70% — the discount is widening, not simply tracking market declines.

**Section 02 — Why: the duck curve deepens every year**
Two side-by-side charts. The left overlays average hourly SP15 price for all three years on one axis, so the deepening midday trough is directly comparable year on year. The right shows average hourly solar generation stacked by year, showing more capacity flooding the same window. Together they explain the mechanism: the price at the peak solar generation hour fell from $96.5/MWh (2023) to $15.2/MWh (2025).

**Section 03 — The next stage: negative prices and value-at-risk**
Two charts. The left shows daily capture ratios as bars for all three years simultaneously — bright bars flag days below 0.40x. The 2025 distribution visibly shifts left, with many more extreme low-capture days. The right shows P10/P50/P90 capture ratios grouped by year, quantifying how the downside tail widens. The P10 fell from 0.54x (2023) to 0.14x (2025) — on the worst 10% of days in January 2025, solar earned only 14 cents per dollar of average market price.

**Section 04 — Curtailment: the hidden revenue loss**
A bar chart of the 30 largest curtailment hours across all three years, coloured by year. Curtailment events are entirely concentrated in the 09:00–16:00 PST solar window. The 2025 bars dwarf 2023 levels, illustrating how curtailment has grown from a minor occurrence to a structural feature of high-solar days. The key point: standard capture price misses this entirely — curtailed MWh earned nothing but are excluded from the standard calculation.

**Section 05 — What this means for market participants**
Five numbered implications covering: PPA pricing accuracy, project finance stress-testing, curtailment as structural risk, the evening ramp as the storage value signal, and the NP15/SP15 hub divergence as a siting consideration.

---

### `solar_capture_dashboard.html` — Interactive explorer

Interactive month-by-month and hub-by-hub analysis with four chart tabs:

- **Fig 1** — Daily capture price vs average LMP time series with solar generation bars
- **Fig 2** — Duck curve: hourly price vs solar generation profile
- **Fig 3** — Hour × day-of-week price heatmap (cannibalization pattern)
- **Fig 4** — Price vs solar generation scatter (quantifies the cannibalization slope via regression)

Toggle between SP15/NP15 and between available months to compare hub and seasonal dynamics.

---

### `yoy_trend.html` — Year-on-year comparison

Focused year-on-year view showing how the capture ratio has trended as solar capacity grows. Includes the duck curve overlay (all three years on one axis), P10/P50/P90 comparison, and negative-price exposure bar chart. Toggle between SP15 and NP15 to see the divergence in hub-level dynamics.

---

### `value_at_risk.html` — Risk distribution

P10/P50/P90 capture ratio distribution across all available daily observations. Includes:
- Histogram of daily capture ratios coloured by severity tier
- Time series with P10/P90 reference lines
- "Bad day anatomy" — average hourly price on P10 days vs P90 days (shows *why* bad days are bad: more solar output pushing prices further negative at the same hours)
- Threshold exceedance table: how many days fell below 0.40x, 0.50x, 0.60x etc.

---

### `curtailment_analysis.html` — Adjusted capture rate

Standard capture price vs curtailment-adjusted capture price, which accounts for generation withheld during oversupply. Includes daily curtailment volume and curtailment rate time series. The adjusted metric shows the true economic impact of cannibalization: not just lower prices, but generation that produced nothing at all.

---

## Metrics reference

| Metric | Formula | What it tells you |
|--------|---------|-------------------|
| Solar capture price | `Σ(LMP × solar_MW) / Σ(solar_MW)` | Revenue-weighted avg price solar actually earns |
| Capture ratio | `capture_price / avg_LMP` | How much less than average solar earns (< 1 = discount) |
| Negative price exposure | `solar_MW during LMP < $0 / total solar_MW` | Fraction of output during negative-price hours |
| Curtailment-adjusted capture | `Σ(LMP × potential_MW) / Σ(potential_MW)` | Capture price including withheld generation |
| P10 capture ratio | 10th percentile of daily ratios | Downside scenario for lenders and stress tests |

---

## Data sources

| Dataset | Source | Query / File |
|---------|--------|--------------|
| NP15 hub LMP | CAISO OASIS API | `PRC_LMP`, DAM, node `TH_NP15_GEN-APND` |
| SP15 hub LMP | CAISO OASIS API | `PRC_LMP`, DAM, node `TH_SP15_GEN-APND` |
| Solar generation | CAISO OASIS API | `SLD_REN_FCST`, `MARKET_RUN_ID=ACTUAL`, `RENEWABLE_TYPE=Solar` |
| Curtailment | [CAISO Production & Curtailments](https://www.caiso.com/library/production-curtailments-data) | Annual XLSX, `Curtailments` sheet, `Solar Curtailment` column |

**Why SP15?** Southern California hub covers ~60% of CAISO's installed solar capacity and is the primary settlement reference for utility-scale solar PPAs in the region.

**Why DAM prices?** Day-Ahead prices are the standard settlement reference for utility-scale solar contracts and less volatile than real-time, making them appropriate for capture price analysis.

**Why January for year-on-year comparison?** January has no DST transition (consistent UTC-8 offset), a stable weather baseline across years, and moderate but meaningful solar penetration. This gives a clean signal of the cannibalization trend without confounding effects from wet/dry year hydro variation (spring) or peak summer generation patterns.

---

## Setup

```bash
git clone https://github.com/yourusername/solar-capture-caiso
cd solar-capture-caiso
pip install -r requirements.txt
```

---

## Running the pipeline

### 1. Download raw market data

```bash
python src/download_data.py
```

Downloads January LMP and solar data from CAISO OASIS API into `data/raw/`. Takes ~2 minutes. For year-on-year analysis, save outputs to `data/yoy/` with naming `lmp_SP15_YYYY.csv`, `lmp_NP15_YYYY.csv`, `solar_YYYY.csv`.

### 2. Process and merge

```bash
python src/process_data.py
```

Merges raw CSVs, converts UTC timestamps to Pacific time, and outputs `data/processed/market_data.csv`. The timezone conversion happens here — all downstream scripts inherit correct PST hours automatically.

### 3. Generate dashboards

```bash
python src/analysis_synthesis.py      # synthesis_dashboard.html
python src/analysis_dashboard.py      # solar_capture_dashboard.html
python src/analysis_yoy.py            # yoy_trend.html
python src/analysis_var.py            # value_at_risk.html
python src/analysis_curtailment.py    # curtailment_analysis.html
```

For curtailment analysis, download annual XLSX files from the [CAISO library](https://www.caiso.com/library/production-curtailments-data) and save as `data/raw/curtailments_YYYY.xlsx`.

---

## Repo structure

```
solar-capture-caiso/
├── src/
│   ├── download_data.py          # CAISO OASIS API client
│   ├── process_data.py           # merge, clean, UTC → PST
│   ├── analysis_synthesis.py     # synthesis narrative dashboard
│   ├── analysis_dashboard.py     # interactive monthly explorer
│   ├── analysis_yoy.py           # year-on-year trend
│   ├── analysis_var.py           # value-at-risk distribution
│   └── analysis_curtailment.py   # curtailment-adjusted capture
├── data/
│   ├── raw/                      # lmp_np15.csv, lmp_sp15.csv, solar_generation.csv
│   │                             # curtailments_YYYY.xlsx
│   ├── processed/                # market_data.csv, curtailment_hourly.csv
│   │                             # market_data_with_curtailment.csv
│   └── yoy/                      # lmp_SP15_YYYY.csv, lmp_NP15_YYYY.csv, solar_YYYY.csv
├── outputs/                      # all generated HTML dashboards
├── notebooks/
│   └── exploration.ipynb         # interactive analysis notebook
├── requirements.txt
└── README.md
```

---

## Known API quirks

- **SSL**: CAISO uses a self-signed certificate. Use `requests.get(..., verify=False)` — switching to `http://` doesn't help because the server redirects back to `https://`
- **`version=1` is required** — omitting it returns `INVALID_REQUEST`
- **LMP requests are capped at 24 hours per call** when using `grp_type=ALL` (returns the entire grid). Pass `node=TH_SP15_GEN-APND` directly as a param to filter server-side and request a full month in one call
- **Filter on `NODE_ID`** not `NODE` when parsing LMP responses
- **Solar dataset**: use `SLD_REN_FCST` with `RENEWABLE_TYPE=Solar, MARKET_RUN_ID=ACTUAL`. The `Curtailments` sheet has five `MARKET_RUN_ID` values (ACTUAL/DAM/HASP/RTD/RTPD) and three `TRADING_HUB` rows per hour — filter then sum across hubs
- **Solar negatives**: `SLD_REN_FCST` returns small negative values during night hours (curtailment accounting residuals). Clip to zero with `.clip(lower=0)`
- **Timezone**: all timestamps are converted to `America/Los_Angeles` in `process_data.py`. Do not pass `utc=True` when re-reading the processed CSV — it will silently reconvert PST → UTC and shift the duck curve by 8 hours
- **Curtailment file format**: 5-minute intervals, sparse (only non-zero rows listed). Sum MW readings per hour and multiply by 5/60 to get MWh, which equals average MW curtailed for that hour
