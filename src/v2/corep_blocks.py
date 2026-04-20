from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import pandas as pd

try:
    from v2.get_RC_value import (
        DEFAULT_BASED_TEMPLATE_PATH,
        DEFAULT_BASED_TEMPLATE_SHEET,
        DEFAULT_COREP_DIR,
        get_value,
        load_based_template,
    )
    from v2.rule_engine import evaluate_rule, evaluate_rules
    from v2.run_pipeline import run_pipeline
    from v2.explain_rule_logic import build_logic_audit
    from v2.create_stub_workbooks import create_stubs
    from v2.seed_corep_values import seed_corep_values
except ModuleNotFoundError:
    from v2.get_RC_value import (  # type: ignore
        DEFAULT_BASED_TEMPLATE_PATH,
        DEFAULT_BASED_TEMPLATE_SHEET,
        DEFAULT_COREP_DIR,
        get_value,
        load_based_template,
    )
    from v2.rule_engine import evaluate_rule, evaluate_rules  # type: ignore
    from v2.run_pipeline import run_pipeline  # type: ignore
    from v2.explain_rule_logic import build_logic_audit  # type: ignore
    from v2.create_stub_workbooks import create_stubs  # type: ignore
    from v2.seed_corep_values import seed_corep_values  # type: ignore


@dataclass(frozen=True)
class BlockSpec:
    name: str
    purpose: str
    required_inputs: List[str]
    outputs: List[str]


def get_block_specs() -> List[BlockSpec]:
    """Return canonical workflow block definitions (beginner-friendly)."""
    return [
        BlockSpec(
            name="block_process_1_extract_values",
            purpose="Extract COREP values from selector inputs (get_RC block)",
            required_inputs=["templates_used", "tables", "rows", "columns", "sheets", "corep_dir"],
            outputs=["{template: {file_path, tables: {table_or_sheet: {sheet_name, dataframe}}}}"],
        ),
        BlockSpec(
            name="block_process_2_prepare_data",
            purpose="Create missing COREP files and seed deterministic test values",
            required_inputs=["config_path", "sheet_name", "corep_dir", "overwrite_seed"],
            outputs=["stubs summary", "seed summary"],
        ),
        BlockSpec(
            name="block_process_3_evaluate_single_rule",
            purpose="Evaluate one config row",
            required_inputs=["rule_row", "data_mapping OR corep_dir"],
            outputs=["rule_id", "status", "reason", "details[]"],
        ),
        BlockSpec(
            name="block_process_4_evaluate_config",
            purpose="Evaluate config rules and return raw list",
            required_inputs=["config_path", "sheet_name", "corep_dir", "max_rules"],
            outputs=["list of rule results"],
        ),
        BlockSpec(
            name="block_process_5_build_outputs",
            purpose="Evaluate config and write output files",
            required_inputs=["config_path", "sheet_name", "corep_dir", "output_dir", "max_rules"],
            outputs=["outputs/rule_results.xlsx", "outputs/rule_results.json", "status counts"],
        ),
        BlockSpec(
            name="block_process_6_explain_logic",
            purpose="Create plain-language rule interpretation workbook",
            required_inputs=["config_path", "sheet_name", "output_path", "max_rules"],
            outputs=["outputs/rule_logic_audit.xlsx", "rules_processed", "mappings_processed"],
        ),
    ]


# ============================================================
# Canonical block process API
# ============================================================


def block_process_1_extract_values(
    templates_used: Any,
    tables: Any = None,
    rows: Any = None,
    columns: Any = None,
    sheets: Any = None,
    corep_dir: Optional[str | Path] = None,
) -> Dict[str, Dict[str, Any]]:
    return get_value(
        templates_used=templates_used,
        tables=tables,
        rows=rows,
        columns=columns,
        sheets=sheets,
        corep_dir=corep_dir,
    )


def block_process_2_prepare_data(
    config_path: Path = DEFAULT_BASED_TEMPLATE_PATH,
    sheet_name: str = DEFAULT_BASED_TEMPLATE_SHEET,
    corep_dir: Path = DEFAULT_COREP_DIR,
    overwrite_seed: bool = False,
) -> Dict[str, Any]:
    return {
        "stubs": create_stubs(config_path=config_path, sheet_name=sheet_name, corep_dir=corep_dir),
        "seed": seed_corep_values(
            config_path=config_path,
            sheet_name=sheet_name,
            corep_dir=corep_dir,
            overwrite=overwrite_seed,
        ),
    }


def block_process_3_evaluate_single_rule(
    rule_row: pd.Series,
    data_mapping: Optional[Mapping[str, Any]] = None,
    corep_dir: str | Path = DEFAULT_COREP_DIR,
) -> Dict[str, Any]:
    return evaluate_rule(rule_row=rule_row, data_mapping=data_mapping, corep_dir=corep_dir)


def block_process_4_evaluate_config(
    config_path: Path = DEFAULT_BASED_TEMPLATE_PATH,
    sheet_name: str = DEFAULT_BASED_TEMPLATE_SHEET,
    corep_dir: Path = DEFAULT_COREP_DIR,
    max_rules: int | None = None,
) -> List[Dict[str, Any]]:
    return evaluate_rules(
        config_path=config_path,
        sheet_name=sheet_name,
        corep_dir=corep_dir,
        max_rules=max_rules,
    )


def block_process_5_build_outputs(
    config_path: Path = DEFAULT_BASED_TEMPLATE_PATH,
    sheet_name: str = DEFAULT_BASED_TEMPLATE_SHEET,
    corep_dir: Path = DEFAULT_COREP_DIR,
    output_dir: Path = Path("outputs"),
    max_rules: int | None = None,
) -> Dict[str, Any]:
    return run_pipeline(
        config_path=config_path,
        sheet_name=sheet_name,
        corep_dir=corep_dir,
        output_dir=output_dir,
        max_rules=max_rules,
    )


def block_process_6_explain_logic(
    config_path: Path = DEFAULT_BASED_TEMPLATE_PATH,
    sheet_name: str = DEFAULT_BASED_TEMPLATE_SHEET,
    output_path: Path = Path("outputs/rule_logic_audit.xlsx"),
    max_rules: int | None = None,
) -> Dict[str, Any]:
    return build_logic_audit(
        config_path=config_path,
        sheet_name=sheet_name,
        output_path=output_path,
        max_rules=max_rules,
    )


def load_config_rules(
    config_path: Path = DEFAULT_BASED_TEMPLATE_PATH,
    sheet_name: str = DEFAULT_BASED_TEMPLATE_SHEET,
) -> pd.DataFrame:
    return load_based_template(config_path, sheet_name)


def run_block_workflow(
    config_path: Path = DEFAULT_BASED_TEMPLATE_PATH,
    sheet_name: str = DEFAULT_BASED_TEMPLATE_SHEET,
    corep_dir: Path = DEFAULT_COREP_DIR,
    output_dir: Path = Path("outputs"),
    max_rules: int | None = None,
    overwrite_seed: bool = False,
) -> Dict[str, Any]:
    """Run the full canonical flow by calling each block process in sequence."""
    prepare_summary = block_process_2_prepare_data(
        config_path=config_path,
        sheet_name=sheet_name,
        corep_dir=corep_dir,
        overwrite_seed=overwrite_seed,
    )

    evaluate_summary = block_process_5_build_outputs(
        config_path=config_path,
        sheet_name=sheet_name,
        corep_dir=corep_dir,
        output_dir=output_dir,
        max_rules=max_rules,
    )

    explain_summary = block_process_6_explain_logic(
        config_path=config_path,
        sheet_name=sheet_name,
        output_path=output_dir / "rule_logic_audit.xlsx",
        max_rules=max_rules,
    )

    return {
        "prepare": prepare_summary,
        "evaluate": evaluate_summary,
        "explain": explain_summary,
    }
