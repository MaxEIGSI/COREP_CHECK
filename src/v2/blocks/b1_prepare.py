"""
Block 1 – Prepare data
======================
Creates any missing COREP workbook stubs and seeds them with deterministic
test values.  Safe to re-run: stubs skip existing files, seed skips filled
cells by default.

Input context keys:  config_path, sheet_name, corep_dir
Output context keys: prepare_summary  {"stubs": {...}, "seed": {...}}
"""
from __future__ import annotations

from typing import Any, Dict

try:
    from v2.create_stub_workbooks import create_stubs
    from v2.seed_corep_values import seed_corep_values
except ModuleNotFoundError:
    from v2.create_stub_workbooks import create_stubs  # type: ignore
    from v2.seed_corep_values import seed_corep_values  # type: ignore


def block_prepare_data(ctx: Dict[str, Any]) -> Dict[str, Any]:
    """Create missing workbook stubs and seed deterministic values."""
    config_path = ctx["config_path"]
    sheet_name  = ctx["sheet_name"]
    corep_dir   = ctx["corep_dir"]
    overwrite   = ctx.get("overwrite_seed", False)

    stubs = create_stubs(
        config_path=config_path,
        sheet_name=sheet_name,
        corep_dir=corep_dir,
    )
    seed = seed_corep_values(
        config_path=config_path,
        sheet_name=sheet_name,
        corep_dir=corep_dir,
        overwrite=overwrite,
    )

    ctx["prepare_summary"] = {"stubs": stubs, "seed": seed}
    return ctx
