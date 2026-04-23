"""
Block 3 – Evaluate rules
========================
Runs every loaded rule against the COREP workbooks and stores raw results.

Input context keys:  config_path, sheet_name, corep_dir, max_rules (optional)
Output context keys: results  (List[Dict])
"""
from __future__ import annotations

from typing import Any, Dict

try:
    from v2.rule_engine import evaluate_rules
except ModuleNotFoundError:
    from v2.rule_engine import evaluate_rules  # type: ignore


def block_evaluate_rules(ctx: Dict[str, Any]) -> Dict[str, Any]:
    """Evaluate all rules and store raw result list in context."""
    ctx["results"] = evaluate_rules(
        config_path=ctx["config_path"],
        sheet_name=ctx["sheet_name"],
        corep_dir=ctx["corep_dir"],
        max_rules=ctx.get("max_rules"),
    )
    return ctx
