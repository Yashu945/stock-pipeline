"""Load gold Parquet files into DuckDB for SQL-based analysis."""

import logging
from pathlib import Path

import duckdb
import yaml

log = logging.getLogger(__name__)

GOLD_TABLES = {
    "enriched_prices": "enriched_prices.parquet",
    "monthly_summary": "monthly_summary.parquet",
    "top_performers": "top_performers.parquet",
}


def load_config(config_path: str = "config/config.yaml") -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f)


def get_connection(db_path: str) -> duckdb.DuckDBPyConnection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(db_path)


def load_gold_to_db(config_path: str = "config/config.yaml") -> duckdb.DuckDBPyConnection:
    cfg = load_config(config_path)
    gold_dir = Path(cfg["paths"]["gold"])
    db_path = cfg["database"]["path"]

    con = get_connection(db_path)

    for table, filename in GOLD_TABLES.items():
        parquet_path = gold_dir / filename
        if not parquet_path.exists():
            log.warning(f"Gold file missing: {parquet_path}")
            continue
        con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM read_parquet('{parquet_path}')")
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        log.info(f"Loaded table '{table}' → {count} rows  (db: {db_path})")

    return con


def query(sql: str, config_path: str = "config/config.yaml"):
    cfg = load_config(config_path)
    con = duckdb.connect(cfg["database"]["path"])
    return con.execute(sql).df()


def print_summary(config_path: str = "config/config.yaml") -> None:
    print("\n=== enriched_prices (last 5 rows) ===")
    print(query("SELECT ticker, date, close, ma_7, ma_30, daily_return FROM enriched_prices ORDER BY date DESC LIMIT 5", config_path).to_string(index=False))

    print("\n=== monthly_summary (last 6 months) ===")
    print(query("SELECT * FROM monthly_summary ORDER BY year_month DESC LIMIT 6", config_path).to_string(index=False))

    print("\n=== top_performers (latest month) ===")
    latest = query("SELECT MAX(year_month) AS m FROM monthly_summary", config_path).iloc[0, 0]
    print(query(f"SELECT ticker, year_month, monthly_return FROM top_performers WHERE year_month = '{latest}'", config_path).to_string(index=False))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    load_gold_to_db()
    print_summary()
