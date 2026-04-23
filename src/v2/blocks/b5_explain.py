"""
Block 5 – Explain rules
=======================
Produces a human-readable rule interpretation workbook with plain-language
descriptions and row-alias mapping tables.

Input context keys:  config_path, sheet_name, output_dir, max_rules (optional)
Output context keys: explain_summary  {"rules_processed", "mappings_processed",
                                        "output_path"}
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

try:
    from v2.explain_rule_logic import build_logic_audit
except ModuleNotFoundError:
    from v2.explain_rule_logic import build_logic_audit  # type: ignore


def block_explain_rules(ctx: Dict[str, Any]) -> Dict[str, Any]:
    """Build a logic-audit workbook explaining each rule in plain language."""
    output_path = Path(ctx.get("output_dir", "src/v2/data/output")) / "rule_logic_audit.xlsx"
    ctx["explain_summary"] = build_logic_audit(
        config_path=ctx["config_path"],
        sheet_name=ctx["sheet_name"],
        output_path=output_path,
        max_rules=ctx.get("max_rules"),
    )
    return ctx
