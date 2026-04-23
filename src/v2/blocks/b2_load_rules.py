"""
Block 2 – Load rules
====================
Reads the config spreadsheet and stores the full rules dataframe in context.

Input context keys:  config_path, sheet_name
Output context keys: rules_df  (pd.DataFrame)
"""
from __future__ import annotations

from typing import Any, Dict

import pandas as pd

try:
    from v2.rule_engine import load_rules
except ModuleNotFoundError:
    from v2.rule_engine import load_rules  # type: ignore


def block_load_rules(ctx: Dict[str, Any]) -> Dict[str, Any]:
    """Load config rules from the Excel workbook into context."""
    ctx["rules_df"] = load_rules(
        config_path=ctx["config_path"],
        sheet_name=ctx["sheet_name"],
    )
    return ctx
