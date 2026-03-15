# Solar Capture Price — CAISO

Analysis of solar capture price dynamics in the California ISO (CAISO) electricity market.

## What this is

Solar assets don't earn the average market price — they generate during midday when their own output suppresses prices. This project quantifies that discount using three metrics:

| Metric | Formula | What it tells you |
|--------|---------|-------------------|
| **Solar capture price** | `Σ(LMP × solar_MW) / Σ(solar_MW)` | Revenue-weighted avg price solar actually earns |
| **Capture ratio** | `capture_price / avg_LMP` | How much less than average solar earns (< 1 = discount) |
| **Negative price exposure** | `solar_MW during LMP < $0 / total solar_MW` | Fraction of output during negative-price hours |

This is a standard framework used by renewable developers pricing PPAs, traders marking solar assets, and analysts at firms like Wood Mackenzie and Modo Energy.

## Outputs

| Chart | Description |
|-------|-------------|
| `capture_price_daily.png` | Daily capture price vs average LMP, with solar generation overlay |
| `duck_curve.png` | Hourly avg generation vs price — shows midday price suppression |
| `price_heatmap.png` | Hour × day-of-week LMP heatmap — reveals cannibalization pattern |
| `capture_ratio_monthly.png` | Monthly capture ratio and negative price exposure |

## Data sources

All data is from the [CAISO OASIS API](http://oasis.caiso.com/oasisapi/SingleZip):

| Dataset | Query | Node/Filter |
|---------|-------|-------------|
| NP15 hub LMP | `PRC_LMP`, DAM | `TH_NP15_GEN-APND` |
| SP15 hub LMP | `PRC_LMP`, DAM | `TH_SP15_GEN-APND` |
| Solar generation | `SLD_REN_FCST`, ACTUAL | `RENEWABLE_TYPE=Solar` |

**Why SP15?** Southern California hub captures ~60% of CAISO's installed solar fleet. The hub price is the most relevant settlement reference for utility-scale solar in that region.

**Why DAM prices?** Day-Ahead prices are the primary settlement reference for most utility-scale solar contracts and are less volatile than real-time, making them the standard for capture price analysis.

## Setup

```bash
git clone https://github.com/yourusername/solar-capture-caiso
cd solar-capture-caiso
pip install -r requirements.txt
```

## Running the pipeline

```bash
# Step 1: download raw data from CAISO OASIS (~2 minutes)
python src/download_data.py

# Step 2: merge and clean datasets
python src/process_data.py

# Step 3: compute metrics and generate plots
python src/analysis.py
```

Outputs land in `outputs/` and `data/processed/`.

## Repo structure

```
solar-capture-caiso/
├── src/
│   ├── download_data.py    # OASIS API client
│   ├── process_data.py     # merge + clean raw CSVs
│   └── analysis.py         # metrics + plots
├── data/
│   ├── raw/                # lmp_np15.csv, lmp_sp15.csv, solar_generation.csv
│   └── processed/          # market_data.csv
├── outputs/                # all generated plots
├── notebooks/
│   └── exploration.ipynb   # interactive analysis
├── requirements.txt
└── README.md
```

## Key findings (Jan 2025)

Solar captured roughly **72–78% of the average SP15 LMP** across the study period. The discount widens as solar penetration grows through spring — March shows a wider gap than January as more solar capacity comes online and midday prices are increasingly suppressed. Negative-price hours, while still a small share of total generation, are a growing concern for unhedged solar projects.

## Known API quirks

- **Use `http://` not `https://`** — OASIS uses a self-signed certificate that fails on macOS/Anaconda
- **`version=1` is required** — omitting it returns `INVALID_REQUEST`
- **Request one month at a time** — daily loops trigger 429 rate limiting
- **Filter on `NODE_ID`** not `NODE` when selecting hub nodes
- **Solar dataset**: use `SLD_REN_FCST` filtered to `RENEWABLE_TYPE=Solar, MARKET_RUN_ID=ACTUAL` — the `ENE_SLRS` dataset has 5-minute intervals and ~1M rows per month
