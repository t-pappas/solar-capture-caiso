# AI process log

A record of how AI was used to build this project and the human interventions that shaped, corrected, and improved it.

---

## How this project was built

This project was built in a single extended session using Claude (Anthropic) as the primary development partner. The workflow was iterative: I described what I wanted, the AI generated code or analysis, and I ran it, inspected outputs, and directed corrections. The AI generated all Python scripts and HTML dashboards. My role was to define the analytical direction, catch errors in real outputs, and push for depth at each stage.

---

## Stage 1 — Scoping and initial pipeline

**What AI did:**
Proposed the project structure, identified the right CAISO OASIS API endpoints (`PRC_LMP`, `SLD_REN_FCST`), wrote the initial `download_data.py`, `process_data.py`, and `analysis.py`, and explained the capture price methodology.

**My interventions:**
- Chose CAISO over the suggested NYISO/ERCOT starting points because CAISO has the highest solar penetration and the cannibalization story is clearest there
- Pushed for SP15 specifically rather than a generic hub, because SP15 is the relevant reference for California solar PPAs
- Asked for the metric framework (capture price, capture ratio, negative-price exposure) to be explicit and formula-driven, not just narrative

---

## Stage 2 — Debugging the CAISO API

This stage had the most back-and-forth. Several issues appeared only when code ran against the real API.

**Issue 1 — SSL certificate failure**
The AI initially suggested switching from `https://` to `http://` to avoid the SSL error. After running the code, it still failed because CAISO redirects `http://` back to `https://`.

*My intervention:* I ran the code, got the same SSL error despite the supposed fix, and pasted the exact traceback showing `HTTPSConnectionPool` in the error despite using `http://` in the URL. The AI diagnosed the redirect and switched to `verify=False` with `urllib3.disable_warnings`.

**Issue 2 — LMP download was extremely slow**
The initial `get_lmp` function used `grp_type=ALL`, which returns every node in CAISO (~thousands of rows per day), then filtered client-side. For a full month of daily requests this took 10+ minutes.

*My intervention:* I flagged that the download was halfway through NP15 after ten minutes with no sign of finishing. The AI diagnosed the root cause — `grp_type=ALL` downloads the entire grid — and rewrote the function to pass the hub node directly as an API parameter, reducing each response from ~100k rows to ~120 rows and collapsing a 31-request loop into a single monthly request. Download time dropped from 10+ minutes to ~10 seconds.

**Issue 3 — Three rows per hour in solar data**
The `solar_generation.csv` was returning three rows per timestamp instead of one, and contained negative values.

*My intervention:* I uploaded the raw CSV so the AI could inspect the actual column structure. This revealed the dataset has three `TRADING_HUB` values (NP15/SP15/SCEZ) per hour — not duplicates but sub-regions that need to be summed. The negatives were identified as curtailment accounting residuals. The fix was `.groupby("timestamp").sum()` followed by `.clip(lower=0)`.

---

## Stage 3 — Timezone bug

**Issue — Duck curve showed solar peaking at hours 17–22 instead of 08–15**
After the pipeline ran successfully and produced data, the duck curve chart showed solar generation peaking in the late evening UTC hours, which is physically wrong for California solar.

*My intervention:* I noticed the duck curve looked wrong visually and reported it. The AI traced it to `pd.to_datetime(..., utc=True)` in `analysis_dashboard.py` — the CSV stored PST timestamps with a `-08:00` offset, but `utc=True` was silently converting them back to UTC before extracting hours, shifting everything by 8 hours. The fix was to remove `utc=True` from the CSV read step. I also pushed to fix this at the `process_data.py` level rather than in each dashboard script individually, so the timezone is handled once and all downstream scripts inherit it correctly.

---

## Stage 4 — Visualization design

**What AI did:**
Built the initial Modo Energy-style dark dashboard after I asked it to research Modo's visual language. Chose the dark background, colour palette (blue for price, red for underperformance, muted green for generation), thin chart lines, inline callout boxes with the key insight written as a sentence.

**My interventions:**
- Asked for NP15/SP15 toggle and month toggle to make the dashboard interactive rather than static
- Flagged that grey fonts were too dark to read on the dark background — pushed for a lighter grey palette and larger font sizes across all dashboards
- Requested a four-tab layout (capture price, duck curve, heatmap, scatter) to keep the main dashboard focused rather than scrolling through multiple charts

---

## Stage 5 — Adding analytical depth

I drove the expansion from a single-month analysis to a full multi-dimensional research project by asking for each new layer:

**Curtailment analysis**
Asked about curtailment after the AI mentioned it as a "next step." I uploaded the CAISO Production & Curtailments XLSX and asked the AI to inspect its structure before writing any code. This revealed the 5-minute sparse format, the `Solar Curtailment` column, and the three-sheet layout. The AI initially assumed a different column structure from the API; inspecting the real file prevented a silent data error. Key decision I made: aggregate by summing MW × (5/60) per hour, not averaging, because the sparse format means missing intervals are zero not null.

**Value-at-risk framing**
I asked for P10/P50/P90 distribution analysis specifically after recognising that averages obscure the project finance risk. The "bad day anatomy" chart (P10 days vs P90 days hourly profile) was my specific request — I wanted to show *why* bad days happen, not just how bad they are.

**Year-on-year analysis**
I provided the YoY data files (lmp_SP15/NP15 and solar for 2023/2024/2025) and asked for the trend to be quantified. I specified January as the comparison month and asked the AI to explain *why* January is the right choice (no DST, stable weather baseline, no hydro confounding) so the methodology is defensible.

---

## Stage 6 — Synthesis dashboard

**What AI did:**
Wrote `analysis_synthesis.py` as a fully dynamic Python script that computes all metrics from source data at runtime, fills a large HTML template using string replacement, and embeds chart data as a JSON payload in the script tag. The five-section narrative structure with callout boxes was designed collaboratively.

**My interventions:**
- Pushed for a narrative-first structure (problem → mechanism → risk → hidden cost → implications) rather than a chart-first layout
- Asked for the conclusion section to be actionable and addressed to specific audiences (developers, lenders, traders) rather than generic
- Specified that all numbers in prose callouts should be computed from real data, not hardcoded — this meant the template approach with `__PLACEHOLDER__` substitution
- Reviewed the curtailment growth figures (50 GWh → 96 GWh → 115 GWh) and noted the 2025 figure came from curtailment data that covers January only, while 2023/2024 figures are also January — asked for this to be made explicit

---

## Key things I caught that AI missed

1. **The http/https redirect loop** — AI assumed switching to http would fix the SSL error. It took me running the code and reading the traceback carefully to notice the URL in the error was still https.

2. **The timezone shift** — The charts ran without errors but were factually wrong. Visual inspection caught it; the AI would not have flagged it automatically.

3. **grp_type=ALL performance** — The AI wrote a working but impractically slow function. I noticed it after 10 minutes of waiting and pushed for a diagnosis.

4. **Curtailment file inspection before writing code** — I insisted on uploading and inspecting the actual XLSX before writing the parser. The AI's initial parser assumed a different column structure that would have produced wrong results silently.

5. **Consistency of timestamp units in curtailment aggregation** — The AI's first pass suggested `.max()` across intervals within each hour. I pushed back on this: the correct aggregation is `.sum() * 5/60` to get MWh, which also equals average MW, matching the units of `solar_mw` in the market data.

---

## What AI contributed that I couldn't have done as quickly alone

- Identified the correct CAISO OASIS query parameters and dataset names from documentation
- Wrote the full ZIP-parsing, retry, and rate-limit handling for the OASIS API
- Designed the Modo-style dark dashboard aesthetic and produced working Chart.js code for six chart types across five dashboards
- Built the `__PLACEHOLDER__` template substitution pattern for the synthesis script, making the HTML fully dynamic
- Explained CAISO market structure (why DAM vs real-time, why SP15, what `MARKET_RUN_ID=ACTUAL` means) when I asked for methodology justification
- Caught and fixed multiple subtle pandas issues (duplicate timestamp handling, timezone-aware merge alignment, period dtype warnings)
