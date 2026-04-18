"""Main pipeline orchestrator: bronze → silver → gold → DuckDB."""

import argparse
import logging
import sys
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def run_stage(name: str, fn, *args, **kwargs):
    log.info(f"{'='*20} Stage: {name} {'='*20}")
    t0 = time.time()
    result = fn(*args, **kwargs)
    log.info(f"Stage '{name}' done in {time.time()-t0:.1f}s")
    return result


def main(config: str = "config/config.yaml", stages: list[str] | None = None):
    from src.ingestion.fetch_stocks import run as ingest
    from src.transformation.bronze_to_silver import run as bronze_to_silver
    from src.transformation.silver_to_gold import run as silver_to_gold
    from src.storage.db import load_gold_to_db, print_summary

    all_stages = ["ingest", "bronze_to_silver", "silver_to_gold", "load_db"]
    stages = stages or all_stages

    if "ingest" in stages:
        run_stage("ingest (bronze)", ingest, config)

    if "bronze_to_silver" in stages:
        run_stage("bronze → silver", bronze_to_silver, config)

    if "silver_to_gold" in stages:
        run_stage("silver → gold", silver_to_gold, config)

    if "load_db" in stages:
        run_stage("load DuckDB", load_gold_to_db, config)
        print_summary(config)

    log.info("Pipeline complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stock data pipeline")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config file")
    parser.add_argument(
        "--stages",
        nargs="+",
        choices=["ingest", "bronze_to_silver", "silver_to_gold", "load_db"],
        help="Run only specific stages (default: all)",
    )
    args = parser.parse_args()
    main(config=args.config, stages=args.stages)
