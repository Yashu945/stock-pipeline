"""Gold layer: business-level aggregations ready for analytics / dashboards."""

import logging
from pathlib import Path

import pandas as pd
import yaml

log = logging.getLogger(__name__)


def load_config(config_path: str = "config/config.yaml") -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f)


def moving_averages(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("date")
    df["ma_7"] = df["close"].rolling(7).mean()
    df["ma_30"] = df["close"].rolling(30).mean()
    df["ma_90"] = df["close"].rolling(90).mean()
    return df


def monthly_summary(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["year_month"] = df["date"].dt.to_period("M").astype(str)
    summary = (
        df.groupby(["ticker", "year_month"])
        .agg(
            open=("open", "first"),
            close=("close", "last"),
            high=("high", "max"),
            low=("low", "min"),
            avg_volume=("volume", "mean"),
            total_volume=("volume", "sum"),
            avg_daily_return=("daily_return", "mean"),
            volatility=("daily_return", "std"),
            trading_days=("date", "count"),
        )
        .reset_index()
    )
    summary["monthly_return"] = (summary["close"] - summary["open"]) / summary["open"]
    return summary


def top_performers(monthly: pd.DataFrame, n: int = 3) -> pd.DataFrame:
    return (
        monthly.sort_values("monthly_return", ascending=False)
        .groupby("year_month")
        .head(n)
        .reset_index(drop=True)
    )


def save_gold(df: pd.DataFrame, name: str, gold_dir: str) -> Path:
    out_dir = Path(gold_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{name}.parquet"
    df.to_parquet(path, index=False)
    log.info(f"Saved gold  → {path}  ({len(df)} rows)")
    return path


def run(config_path: str = "config/config.yaml") -> list[Path]:
    cfg = load_config(config_path)
    tickers = cfg["stocks"]["tickers"]
    silver_dir = Path(cfg["paths"]["silver"])
    gold_dir = cfg["paths"]["gold"]

    frames = []
    for ticker in tickers:
        path = silver_dir / ticker / f"{ticker}_clean.parquet"
        if not path.exists():
            log.warning(f"No silver file for {ticker}")
            continue
        df = pd.read_parquet(path)
        df = moving_averages(df)
        frames.append(df)

    if not frames:
        log.error("No silver data found. Run bronze→silver first.")
        return []

    combined = pd.concat(frames, ignore_index=True)
    monthly = monthly_summary(combined)
    top = top_performers(monthly)

    saved = [
        save_gold(combined, "enriched_prices", gold_dir),
        save_gold(monthly, "monthly_summary", gold_dir),
        save_gold(top, "top_performers", gold_dir),
    ]
    return saved


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run()
