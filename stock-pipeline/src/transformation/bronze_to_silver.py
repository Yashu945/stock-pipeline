"""Silver layer: clean and normalise raw bronze Parquet files."""

import logging
from pathlib import Path

import pandas as pd
import yaml

log = logging.getLogger(__name__)


def load_config(config_path: str = "config/config.yaml") -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f)


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Normalise column names
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    # Parse date
    date_col = "date" if "date" in df.columns else "datetime"
    df[date_col] = pd.to_datetime(df[date_col], utc=True)
    df = df.rename(columns={date_col: "date"})
    df["date"] = df["date"].dt.tz_localize(None)  # store as tz-naive

    # Drop rows missing OHLCV
    required = ["open", "high", "low", "close", "volume"]
    df = df.dropna(subset=required)

    # Remove obvious bad prices
    df = df[(df["close"] > 0) & (df["volume"] >= 0)]

    # Sort
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    # Daily return
    df["daily_return"] = df.groupby("ticker")["close"].pct_change()

    return df


def save_silver(df: pd.DataFrame, ticker: str, silver_dir: str) -> Path:
    out_dir = Path(silver_dir) / ticker
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{ticker}_clean.parquet"
    df.to_parquet(path, index=False)
    log.info(f"Saved silver → {path}  ({len(df)} rows)")
    return path


def run(config_path: str = "config/config.yaml") -> list[Path]:
    cfg = load_config(config_path)
    tickers = cfg["stocks"]["tickers"]
    bronze_dir = Path(cfg["paths"]["bronze"])
    silver_dir = cfg["paths"]["silver"]

    saved = []
    for ticker in tickers:
        files = sorted((bronze_dir / ticker).glob("*.parquet"))
        if not files:
            log.warning(f"No bronze files for {ticker}")
            continue
        # Combine all bronze files for this ticker (handles re-runs)
        df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
        df = df.drop_duplicates(subset=["ticker", df.columns[0]])  # deduplicate by date
        df_clean = clean(df)
        saved.append(save_silver(df_clean, ticker, silver_dir))
    return saved


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run()
