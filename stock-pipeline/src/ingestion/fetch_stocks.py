"""Bronze layer: fetch raw stock data from Yahoo Finance and persist as Parquet."""

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml
import yfinance as yf

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def load_config(config_path: str = "config/config.yaml") -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f)


def fetch_ticker(ticker: str, period: str, interval: str) -> pd.DataFrame:
    log.info(f"Fetching {ticker} | period={period} interval={interval}")
    df = yf.download(ticker, period=period, interval=interval, auto_adjust=True, progress=False)
    if df.empty:
        log.warning(f"No data returned for {ticker}")
        return pd.DataFrame()
    df = df.reset_index()
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    df["ticker"] = ticker
    df["ingested_at"] = datetime.utcnow().isoformat()
    return df


def save_bronze(df: pd.DataFrame, ticker: str, bronze_dir: str) -> Path:
    out_dir = Path(bronze_dir) / ticker
    out_dir.mkdir(parents=True, exist_ok=True)
    date_tag = datetime.utcnow().strftime("%Y%m%d")
    path = out_dir / f"{ticker}_{date_tag}.parquet"
    df.to_parquet(path, index=False)
    log.info(f"Saved bronze → {path}  ({len(df)} rows)")
    return path


def run(config_path: str = "config/config.yaml") -> list[Path]:
    cfg = load_config(config_path)
    tickers = cfg["stocks"]["tickers"]
    period = cfg["stocks"]["period"]
    interval = cfg["stocks"]["interval"]
    bronze_dir = cfg["paths"]["bronze"]

    saved = []
    for ticker in tickers:
        df = fetch_ticker(ticker, period, interval)
        if not df.empty:
            saved.append(save_bronze(df, ticker, bronze_dir))
    return saved


if __name__ == "__main__":
    run()
