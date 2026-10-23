#!/usr/bin/env python3
"""Compatibility entrypoint for the Adobooru orchestrator.

The implementation now lives in the :mod:`adobooru` package.  This module
keeps the historical ``python ingest_orchestrator.py ...`` command and the
``import ingest_orchestrator`` interface working for existing scripts.
"""

from __future__ import annotations

from adobooru import *
from adobooru import __version__  # noqa: F401
from adobooru.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
