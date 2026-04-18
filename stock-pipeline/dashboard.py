"""Generate a 4-panel stock dashboard from gold-layer Parquet files."""

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
import seaborn as sns
from pathlib import Path

GOLD = Path("data/gold")
OUT  = Path("data/dashboard.png")

COLORS = {
    "AAPL":  "#007AFF",
    "GOOGL": "#34A853",
    "MSFT":  "#F25022",
    "AMZN":  "#FF9900",
    "TSLA":  "#CC0000",
    "AMD":   "#ED1C24",
}

# ── load ──────────────────────────────────────────────────────────────────────
prices  = pd.read_parquet(GOLD / "enriched_prices.parquet")
monthly = pd.read_parquet(GOLD / "monthly_summary.parquet")
top     = pd.read_parquet(GOLD / "top_performers.parquet")

prices["date"] = pd.to_datetime(prices["date"])
tickers = sorted(prices["ticker"].unique())

# ── canvas ────────────────────────────────────────────────────────────────────
sns.set_theme(style="darkgrid", palette="muted")
fig = plt.figure(figsize=(20, 14), facecolor="#0F1117")
fig.suptitle("Stock Dashboard  •  1-Year Overview", fontsize=22,
             fontweight="bold", color="white", y=0.98)

axes_color   = "#1C1E26"
text_color   = "#EAEAEA"
grid_color   = "#2A2D3A"

def style_ax(ax, title):
    ax.set_facecolor(axes_color)
    ax.set_title(title, color=text_color, fontsize=13, fontweight="bold", pad=10)
    ax.tick_params(colors=text_color)
    ax.xaxis.label.set_color(text_color)
    ax.yaxis.label.set_color(text_color)
    for spine in ax.spines.values():
        spine.set_edgecolor(grid_color)
    ax.grid(color=grid_color, linewidth=0.6)

gs = fig.add_gridspec(2, 2, hspace=0.38, wspace=0.28,
                      left=0.06, right=0.97, top=0.93, bottom=0.07)

# ── Panel 1 — Closing Price + MA30 ───────────────────────────────────────────
ax1 = fig.add_subplot(gs[0, 0])
style_ax(ax1, "Closing Price  (with 30-day Moving Avg)")
for ticker in tickers:
    df = prices[prices["ticker"] == ticker].sort_values("date")
    c  = COLORS[ticker]
    ax1.plot(df["date"], df["close"], color=c, linewidth=1.4, alpha=0.9, label=ticker)
    ax1.plot(df["date"], df["ma_30"], color=c, linewidth=0.7, linestyle="--", alpha=0.5)
ax1.yaxis.set_major_formatter(mticker.StrMethodFormatter("${x:,.0f}"))
ax1.legend(facecolor=axes_color, labelcolor=text_color, fontsize=9, framealpha=0.8)

# ── Panel 2 — Monthly Returns Heatmap ────────────────────────────────────────
ax2 = fig.add_subplot(gs[0, 1])
style_ax(ax2, "Monthly Return %  (heatmap)")
pivot = monthly.pivot(index="ticker", columns="year_month", values="monthly_return") * 100
# keep last 12 months
pivot = pivot[sorted(pivot.columns)[-12:]]
sns.heatmap(
    pivot, ax=ax2, cmap="RdYlGn", center=0, annot=True, fmt=".1f",
    linewidths=0.4, linecolor=axes_color,
    annot_kws={"size": 7.5},
    cbar_kws={"shrink": 0.8, "label": "Return %"},
)
ax2.set_xlabel("")
ax2.set_ylabel("")
ax2.tick_params(axis="x", rotation=45, labelsize=7.5)
ax2.tick_params(axis="y", rotation=0,  labelsize=9, colors=text_color)
ax2.collections[0].colorbar.ax.tick_params(colors=text_color)
ax2.collections[0].colorbar.ax.yaxis.label.set_color(text_color)

# ── Panel 3 — Daily Return Distribution ──────────────────────────────────────
ax3 = fig.add_subplot(gs[1, 0])
style_ax(ax3, "Daily Return Distribution")
for ticker in tickers:
    df = prices[prices["ticker"] == ticker].dropna(subset=["daily_return"])
    ax3.hist(df["daily_return"] * 100, bins=40, alpha=0.55,
             color=COLORS[ticker], label=ticker, edgecolor="none")
ax3.axvline(0, color="white", linewidth=0.8, linestyle="--", alpha=0.6)
ax3.set_xlabel("Daily Return (%)")
ax3.set_ylabel("Days")
ax3.legend(facecolor=axes_color, labelcolor=text_color, fontsize=9, framealpha=0.8)

# ── Panel 4 — Avg Monthly Volume ─────────────────────────────────────────────
ax4 = fig.add_subplot(gs[1, 1])
style_ax(ax4, "Avg Monthly Volume  (last 6 months)")
recent = monthly[monthly["year_month"].isin(sorted(monthly["year_month"].unique())[-6:])]
vol_pivot = recent.pivot(index="year_month", columns="ticker", values="avg_volume") / 1e6
x     = range(len(vol_pivot))
width = 0.15
for i, ticker in enumerate(tickers):
    if ticker in vol_pivot.columns:
        offset = (i - len(tickers)/2) * width
        ax4.bar([xi + offset for xi in x], vol_pivot[ticker],
                width=width, color=COLORS[ticker], label=ticker, alpha=0.85)
ax4.set_xticks(list(x))
ax4.set_xticklabels(vol_pivot.index, rotation=30, ha="right", fontsize=8)
ax4.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:.0f}M"))
ax4.set_ylabel("Avg Daily Volume")
ax4.legend(facecolor=axes_color, labelcolor=text_color, fontsize=9, framealpha=0.8)

# ── save ──────────────────────────────────────────────────────────────────────
OUT.parent.mkdir(exist_ok=True)
fig.savefig(OUT, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
print(f"Dashboard saved → {OUT.resolve()}")
