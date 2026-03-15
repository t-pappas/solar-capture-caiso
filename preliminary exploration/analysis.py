"""
analysis.py
-----------
Computes solar capture price metrics and generates publication-quality plots.

Metrics:
    1. Solar capture price  = Σ(sp15_lmp × solar_mw) / Σ(solar_mw)
    2. Capture ratio        = capture_price / average_sp15_lmp
    3. Negative price exposure = solar_mw where sp15_lmp < 0 / total solar_mw

Outputs (saved to outputs/):
    capture_price_daily.png     — daily capture vs average price time series
    duck_curve.png              — hourly avg generation vs price overlay
    price_heatmap.png           — hour-of-day × day-of-week price grid
    capture_ratio_monthly.png   — monthly summary bar chart
"""

from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
from matplotlib.colors import TwoSlopeNorm

ROOT      = Path(__file__).resolve().parents[1]
PROC_DIR  = ROOT / "data" / "processed"
OUT_DIR   = ROOT / "outputs"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------------------------------------------------------
# Colour palette — clean, professional
# ----------------------------------------------------------------------------
C_BLUE    = "#185FA5"   # average LMP line
C_RED     = "#E24B4A"   # capture price line
C_GREEN   = "#639922"   # solar generation fill
C_GRAY    = "#888780"   # secondary elements
C_BG      = "#FAFAF8"   # chart background
C_GRID    = "#E8E6DF"   # grid lines


# ----------------------------------------------------------------------------
# Metric calculations
# ----------------------------------------------------------------------------

def compute_metrics(df: pd.DataFrame) -> dict:
    """Compute all three capture metrics on the full dataset."""

    total_gen = df["solar_mw"].sum()

    capture_price = (df["sp15_lmp"] * df["solar_mw"]).sum() / total_gen
    avg_price     = df["sp15_lmp"].mean()
    capture_ratio = capture_price / avg_price

    neg_mask = df["sp15_lmp"] < 0
    neg_exposure = df.loc[neg_mask, "solar_mw"].sum() / total_gen

    return {
        "capture_price":   round(capture_price, 2),
        "avg_sp15_lmp":    round(avg_price, 2),
        "capture_ratio":   round(capture_ratio, 4),
        "neg_exposure":    round(neg_exposure, 4),
        "total_solar_gwh": round(total_gen / 1000, 1),
        "neg_hours":       int(neg_mask.sum()),
    }


def compute_daily(df: pd.DataFrame) -> pd.DataFrame:
    """Daily capture price, average LMP, and total solar generation."""
    df = df.copy()
    df["date"] = df["timestamp"].dt.date

    daily = df.groupby("date").apply(
        lambda g: pd.Series({
            "capture_price": (g["sp15_lmp"] * g["solar_mw"]).sum() / g["solar_mw"].sum()
                             if g["solar_mw"].sum() > 0 else np.nan,
            "avg_lmp":       g["sp15_lmp"].mean(),
            "solar_gwh":     g["solar_mw"].sum() / 1000,
        })
    ).reset_index()
    daily["date"] = pd.to_datetime(daily["date"])
    return daily


def compute_hourly_profile(df: pd.DataFrame) -> pd.DataFrame:
    """Average price and generation by hour-of-day — the duck curve data."""
    df = df.copy()
    df["hour"] = df["timestamp"].dt.hour
    return df.groupby("hour").agg(
        avg_lmp=("sp15_lmp",  "mean"),
        avg_mw= ("solar_mw",  "mean"),
    ).reset_index()


def compute_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """Monthly capture metrics."""
    df = df.copy()
    df["month"] = df["timestamp"].dt.to_period("M")

    rows = []
    for month, g in df.groupby("month"):
        total = g["solar_mw"].sum()
        if total == 0:
            continue
        cap   = (g["sp15_lmp"] * g["solar_mw"]).sum() / total
        avg   = g["sp15_lmp"].mean()
        neg   = g.loc[g["sp15_lmp"] < 0, "solar_mw"].sum() / total
        rows.append({"month": str(month), "capture_price": cap, "avg_lmp": avg,
                     "capture_ratio": cap / avg, "neg_exposure": neg * 100})
    return pd.DataFrame(rows)


# ----------------------------------------------------------------------------
# Plot helpers
# ----------------------------------------------------------------------------

def _style_ax(ax, title: str, xlabel: str = "", ylabel: str = ""):
    ax.set_facecolor(C_BG)
    ax.set_title(title, fontsize=12, fontweight="normal", pad=10, color="#2C2C2A")
    ax.set_xlabel(xlabel, fontsize=10, color=C_GRAY)
    ax.set_ylabel(ylabel, fontsize=10, color=C_GRAY)
    ax.tick_params(colors=C_GRAY, labelsize=9)
    ax.spines[["top", "right"]].set_visible(False)
    ax.spines[["left", "bottom"]].set_color(C_GRID)
    ax.grid(axis="y", color=C_GRID, linewidth=0.7, zorder=0)


# ----------------------------------------------------------------------------
# Plot 1 — Daily capture price vs average price
# ----------------------------------------------------------------------------

def plot_capture_daily(daily: pd.DataFrame, metrics: dict):
    fig, ax1 = plt.subplots(figsize=(12, 5))
    fig.patch.set_facecolor(C_BG)

    # Generation bars (background, secondary axis)
    ax2 = ax1.twinx()
    ax2.bar(daily["date"], daily["solar_gwh"], color=C_GREEN, alpha=0.18,
            width=0.8, zorder=1, label="Solar generation (GWh)")
    ax2.set_ylabel("Solar generation (GWh/day)", fontsize=10, color=C_GREEN)
    ax2.tick_params(axis="y", colors=C_GREEN, labelsize=9)
    ax2.spines[["top", "right"]].set_color(C_GRID)
    ax2.set_ylim(0, daily["solar_gwh"].max() * 3.5)

    # Price lines (foreground)
    ax1.plot(daily["date"], daily["avg_lmp"],       color=C_BLUE,  lw=1.8,
             label=f"Avg SP15 LMP (${metrics['avg_sp15_lmp']:.1f}/MWh)", zorder=3)
    ax1.plot(daily["date"], daily["capture_price"], color=C_RED,   lw=1.8,
             label=f"Solar capture price (${metrics['capture_price']:.1f}/MWh)", zorder=3)
    ax1.axhline(0, color=C_GRAY, lw=0.8, ls="--", alpha=0.6, zorder=2)

    # Shaded gap
    ax1.fill_between(daily["date"], daily["capture_price"], daily["avg_lmp"],
                     where=daily["avg_lmp"] > daily["capture_price"],
                     alpha=0.07, color=C_RED, zorder=2, label="Value discount")

    _style_ax(ax1, "Solar capture price vs average SP15 LMP — daily",
              ylabel="Price ($/MWh)")
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax1.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=30, ha="right")

    # Annotation box
    discount = (1 - metrics["capture_ratio"]) * 100
    ax1.annotate(
        f"Capture ratio: {metrics['capture_ratio']:.1%}\n"
        f"Value discount: {discount:.1f}%\n"
        f"Neg-price exposure: {metrics['neg_exposure']:.1%}",
        xy=(0.02, 0.97), xycoords="axes fraction",
        va="top", ha="left", fontsize=9, color="#2C2C2A",
        bbox=dict(boxstyle="round,pad=0.5", facecolor="white", edgecolor=C_GRID, alpha=0.9),
    )

    # Combined legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2,
               fontsize=9, loc="upper right", framealpha=0.9, edgecolor=C_GRID)

    fig.tight_layout()
    path = OUT_DIR / "capture_price_daily.png"
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=C_BG)
    plt.close(fig)
    print(f"  Saved: {path.name}")


# ----------------------------------------------------------------------------
# Plot 2 — Duck curve
# ----------------------------------------------------------------------------

def plot_duck_curve(hourly: pd.DataFrame):
    fig, ax1 = plt.subplots(figsize=(10, 5))
    fig.patch.set_facecolor(C_BG)

    hours = hourly["hour"]
    ax2 = ax1.twinx()

    # Solar bars
    ax2.bar(hours, hourly["avg_mw"] / 1000, color=C_GREEN, alpha=0.3,
            width=0.7, zorder=1, label="Avg solar gen (GW)")
    ax2.set_ylabel("Average solar generation (GW)", fontsize=10, color=C_GREEN)
    ax2.tick_params(axis="y", colors=C_GREEN, labelsize=9)
    ax2.spines[["top", "right"]].set_color(C_GRID)
    ax2.set_ylim(0, hourly["avg_mw"].max() / 1000 * 4)

    # Price line
    ax1.plot(hours, hourly["avg_lmp"], color=C_BLUE, lw=2.2, zorder=3,
             label="Avg SP15 LMP ($/MWh)")
    ax1.fill_between(hours, hourly["avg_lmp"], alpha=0.08, color=C_BLUE, zorder=2)
    ax1.axhline(0, color=C_GRAY, lw=0.8, ls="--", alpha=0.6, zorder=2)

    _style_ax(ax1, "The duck curve — hourly price vs solar generation",
              xlabel="Hour of day (UTC)", ylabel="Avg price ($/MWh)")
    ax1.set_xticks(range(0, 24, 2))
    ax1.set_xticklabels([f"{h:02d}:00" for h in range(0, 24, 2)], rotation=30)

    # Annotation
    peak_hour  = hourly.loc[hourly["avg_mw"].idxmax(), "hour"]
    trough_lmp = hourly.loc[hourly["avg_lmp"].idxmin(), "avg_lmp"]
    ax1.annotate(
        f"Solar peak: hour {peak_hour:02d}:00\nMin price: ${trough_lmp:.0f}/MWh",
        xy=(peak_hour, trough_lmp), xytext=(peak_hour + 2, trough_lmp + 10),
        arrowprops=dict(arrowstyle="->", color=C_GRAY, lw=0.8),
        fontsize=9, color="#2C2C2A",
        bbox=dict(boxstyle="round,pad=0.4", facecolor="white", edgecolor=C_GRID, alpha=0.9),
    )

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2,
               fontsize=9, loc="upper left", framealpha=0.9, edgecolor=C_GRID)

    fig.tight_layout()
    path = OUT_DIR / "duck_curve.png"
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=C_BG)
    plt.close(fig)
    print(f"  Saved: {path.name}")


# ----------------------------------------------------------------------------
# Plot 3 — Hour × day-of-week price heatmap
# ----------------------------------------------------------------------------

def plot_heatmap(df: pd.DataFrame):
    df = df.copy()
    df["hour"] = df["timestamp"].dt.hour
    df["dow"]  = df["timestamp"].dt.dayofweek   # 0=Mon … 6=Sun

    pivot = df.pivot_table(values="sp15_lmp", index="dow", columns="hour", aggfunc="mean")
    pivot.index = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    # Centre the diverging colormap on zero so negative prices show clearly
    vmin, vmax = pivot.values.min(), pivot.values.max()
    norm = TwoSlopeNorm(vmin=vmin, vcenter=max(0, vmin + 0.01), vmax=vmax)

    fig, ax = plt.subplots(figsize=(14, 4.5))
    fig.patch.set_facecolor(C_BG)
    ax.set_facecolor(C_BG)

    im = ax.imshow(pivot.values, aspect="auto", cmap="RdYlBu_r", norm=norm)

    ax.set_xticks(range(24))
    ax.set_xticklabels([f"{h:02d}" for h in range(24)], fontsize=8)
    ax.set_yticks(range(7))
    ax.set_yticklabels(pivot.index, fontsize=9)
    ax.set_title("SP15 LMP by hour of day and day of week ($/MWh)",
                 fontsize=12, fontweight="normal", pad=10, color="#2C2C2A")
    ax.set_xlabel("Hour of day (UTC)", fontsize=10, color=C_GRAY)
    ax.tick_params(colors=C_GRAY)
    ax.spines[:].set_visible(False)

    cbar = fig.colorbar(im, ax=ax, pad=0.02, fraction=0.03)
    cbar.set_label("$/MWh", fontsize=9, color=C_GRAY)
    cbar.ax.tick_params(labelsize=8, colors=C_GRAY)

    fig.tight_layout()
    path = OUT_DIR / "price_heatmap.png"
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=C_BG)
    plt.close(fig)
    print(f"  Saved: {path.name}")


# ----------------------------------------------------------------------------
# Plot 4 — Monthly capture ratio bar chart
# ----------------------------------------------------------------------------

def plot_monthly(monthly: pd.DataFrame):
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.5))
    fig.patch.set_facecolor(C_BG)

    months = monthly["month"].tolist()
    x = np.arange(len(months))

    # Left: capture price vs avg LMP
    ax = axes[0]
    ax.bar(x - 0.2, monthly["avg_lmp"],       0.35, label="Avg SP15 LMP", color=C_BLUE, alpha=0.8)
    ax.bar(x + 0.2, monthly["capture_price"],  0.35, label="Capture price", color=C_RED,  alpha=0.8)
    _style_ax(ax, "Capture price vs average LMP — monthly", ylabel="$/MWh")
    ax.set_xticks(x)
    ax.set_xticklabels(months, rotation=20)
    ax.legend(fontsize=9, framealpha=0.9, edgecolor=C_GRID)

    # Right: capture ratio + negative price exposure
    ax2 = axes[1]
    bar_colors = [C_RED if r < 0.75 else "#EF9F27" if r < 0.85 else C_GREEN
                  for r in monthly["capture_ratio"]]
    bars = ax2.bar(x, monthly["capture_ratio"] * 100, color=bar_colors, alpha=0.85)
    ax2.axhline(100, color=C_GRAY, lw=0.8, ls="--", alpha=0.5)
    ax2.axhline(80,  color="#EF9F27", lw=0.8, ls=":",  alpha=0.7)

    # Value labels on bars
    for bar, ratio in zip(bars, monthly["capture_ratio"]):
        ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                 f"{ratio * 100:.1f}%", ha="center", va="bottom", fontsize=9, color="#2C2C2A")

    _style_ax(ax2, "Capture ratio — monthly", ylabel="Capture ratio (%)")
    ax2.set_xticks(x)
    ax2.set_xticklabels(months, rotation=20)
    ax2.set_ylim(0, 115)
    ax2.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))

    # Negative price exposure as secondary line
    ax3 = ax2.twinx()
    ax3.plot(x, monthly["neg_exposure"], color=C_GRAY, lw=1.5, ls="--",
             marker="o", ms=5, label="Neg-price exposure (%)", zorder=5)
    ax3.set_ylabel("Neg-price exposure (%)", fontsize=9, color=C_GRAY)
    ax3.tick_params(axis="y", colors=C_GRAY, labelsize=8)
    ax3.spines[["top", "right"]].set_color(C_GRID)
    ax3.legend(fontsize=9, loc="upper right", framealpha=0.9, edgecolor=C_GRID)

    fig.tight_layout()
    path = OUT_DIR / "capture_ratio_monthly.png"
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=C_BG)
    plt.close(fig)
    print(f"  Saved: {path.name}")


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

def main():
    print("=" * 50)
    print("analysis.py — CAISO solar capture metrics")
    print("=" * 50)

    data_path = PROC_DIR / "market_data.csv"
    if not data_path.exists():
        raise FileNotFoundError(
            f"Processed data not found at {data_path}. "
            "Run process_data.py first."
        )

    df = pd.read_csv(data_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    print(f"\nLoaded {len(df)} rows from market_data.csv")

    # -------------------------------------------------------------------------
    print("\nCapture metrics (full period):")
    metrics = compute_metrics(df)
    print(f"  Solar capture price   : ${metrics['capture_price']:.2f}/MWh")
    print(f"  Average SP15 LMP      : ${metrics['avg_sp15_lmp']:.2f}/MWh")
    print(f"  Capture ratio         :  {metrics['capture_ratio']:.1%}")
    print(f"  Value discount        :  {(1 - metrics['capture_ratio']) * 100:.1f}%")
    print(f"  Negative-price expo.  :  {metrics['neg_exposure']:.1%}  ({metrics['neg_hours']} hours)")
    print(f"  Total solar output    :  {metrics['total_solar_gwh']} GWh")

    # -------------------------------------------------------------------------
    print("\nGenerating plots...")
    daily   = compute_daily(df)
    hourly  = compute_hourly_profile(df)
    monthly = compute_monthly(df)

    plot_capture_daily(daily, metrics)
    plot_duck_curve(hourly)
    plot_heatmap(df)
    plot_monthly(monthly)

    print(f"\nAll outputs saved to: {OUT_DIR}/")


if __name__ == "__main__":
    main()
