"""
run_pipeline.py – Single entry point for the COREP rule engine.
================================================================
HOW TO USE
----------
1. Edit the PARAMETERS section below.
2. Toggle blocks on/off with the RUN_* flags.
3. Run:  python -m v2.run_pipeline
         (or open this file and press Run in your IDE)

PIPELINE FLOW
-------------
  context
    → [B1] block_prepare_data    – create stub workbooks + seed values
    → [B2] block_load_rules      – read config rules from Excel
    → [B3] block_evaluate_rules  – run rules against COREP files
    → [B4] block_build_outputs   – write rule_results.xlsx + .json
    → [B5] block_explain_rules   – write rule_logic_audit.xlsx (optional)

FILE MAP
--------
  run_pipeline.py              ← YOU ARE HERE (entry point)
  blocks/b1_prepare.py         ← stub + seed orchestration
  blocks/b2_load_rules.py      ← load config spreadsheet
  blocks/b3_evaluate.py        ← call rule engine
  blocks/b4_build_outputs.py   ← write Excel / JSON outputs
  blocks/b5_explain.py         ← human-readable logic audit
  rule_engine.py               ← core engine (parsing + evaluation)
  get_RC_value.py              ← low-level cell extraction utilities
  create_stub_workbooks.py     ← generate missing COREP workbooks
  seed_corep_values.py         ← fill workbooks with test values
  explain_rule_logic.py        ← plain-language rule interpretation
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

try:
    from v2.get_RC_value import DEFAULT_BASED_TEMPLATE_PATH, DEFAULT_BASED_TEMPLATE_SHEET, DEFAULT_COREP_DIR
    from v2.blocks import (
        block_prepare_data,
        block_load_rules,
        block_evaluate_rules,
        block_build_outputs,
        block_explain_rules,
    )
except ModuleNotFoundError:
    from v2.get_RC_value import DEFAULT_BASED_TEMPLATE_PATH, DEFAULT_BASED_TEMPLATE_SHEET, DEFAULT_COREP_DIR  # type: ignore
    from v2.blocks import (  # type: ignore
        block_prepare_data,
        block_load_rules,
        block_evaluate_rules,
        block_build_outputs,
        block_explain_rules,
    )

# ============================================================
# PARAMETERS – edit these to control what the pipeline does
# ============================================================

CONFIG_PATH = DEFAULT_BASED_TEMPLATE_PATH   # path to EGDQ config workbook
SHEET_NAME  = DEFAULT_BASED_TEMPLATE_SHEET  # sheet inside config workbook
COREP_DIR   = DEFAULT_COREP_DIR             # folder with G_EU_C_*.xlsx files
OUTPUT_DIR  = Path("src/v2/data/output")    # where rule_results.* are written
MAX_RULES   = None                          # int to limit (e.g. 50), None = all

# ============================================================
# BLOCK TOGGLES – set to False to skip a block
# ============================================================

RUN_PREPARE  = True   # create missing stub workbooks + seed test values
RUN_EVALUATE = True   # evaluate rules and write outputs
RUN_EXPLAIN  = False  # write rule_logic_audit.xlsx (slower, optional)

# ============================================================


def run() -> Dict[str, Any]:
    ctx: Dict[str, Any] = {
        "config_path": CONFIG_PATH,
        "sheet_name":  SHEET_NAME,
        "corep_dir":   COREP_DIR,
        "output_dir":  OUTPUT_DIR,
        "max_rules":   MAX_RULES,
    }

    if RUN_PREPARE:
        ctx = block_prepare_data(ctx)
        print("[1] Prepare:", ctx["prepare_summary"])

    ctx = block_load_rules(ctx)
    print(f"[2] Rules loaded: {len(ctx['rules_df'])} rows")

    if RUN_EVALUATE:
        ctx = block_evaluate_rules(ctx)
        ctx = block_build_outputs(ctx)
        print("[3] Evaluate:", ctx["evaluate_summary"])

    if RUN_EXPLAIN:
        ctx = block_explain_rules(ctx)
        print("[4] Explain:", ctx["explain_summary"])

    return ctx


if __name__ == "__main__":
    result = run()
    print("\nDone.")


