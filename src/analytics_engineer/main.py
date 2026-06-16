"""Entry point: run one Medallion stage, or all of them in order.

python -m analytics_engineer.main --stage bronze
python -m analytics_engineer.main --stage all      # default
"""

from __future__ import annotations

import argparse
import logging

from analytics_engineer.data_sources import run_bronze
from analytics_engineer.infra.config_handler import get_config, load_config, set_config
from analytics_engineer.infra.logging import configure_logging
from analytics_engineer.infra.spark import SparkSessionHandler, get_spark
from analytics_engineer.products.frequentation import run_gold, run_silver

logger = logging.getLogger(__name__)

STAGES = {
    "bronze": run_bronze,
    "silver": run_silver,
    "gold": run_gold,
}


def run(stage: str = "all", config_path: str | None = None) -> None:
    configure_logging()
    config = load_config(config_path) if config_path else get_config()
    set_config(config)
    SparkSessionHandler.configure(config.app_name)
    spark = get_spark()
    try:
        for name in STAGES if stage == "all" else [stage]:
            logger.info("running stage: %s", name)
            STAGES[name](spark)
    finally:
        SparkSessionHandler.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Medallion pipeline.")
    parser.add_argument(
        "--stage",
        choices=[*STAGES, "all"],
        default="all",
        help="Stage to run (default: all, in Bronze→Silver→Gold order).",
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Absolute path to config.yaml on the Databricks workspace.",
    )
    args = parser.parse_args()
    run(args.stage, config_path=args.config)


if __name__ == "__main__":
    main()