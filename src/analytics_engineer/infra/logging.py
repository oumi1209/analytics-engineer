"""Logging setup for the pipeline.

Modules log through ``logging.getLogger(__name__)``; the entry point calls
:func:`configure_logging` once so messages are formatted consistently. On
Databricks, where the driver already configures logging, ``basicConfig`` is a
no-op and the workspace's handlers are used instead.
"""

from __future__ import annotations

import logging

_FORMAT = "%(asctime)s %(levelname)-7s %(name)s | %(message)s"


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(level=level, format=_FORMAT)
