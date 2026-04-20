from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

try:
    from v2.get_RC_value import DEFAULT_BASED_TEMPLATE_PATH, DEFAULT_BASED_TEMPLATE_SHEET, DEFAULT_COREP_DIR
    from v2.corep_blocks import (
        block_process_1_extract_values,
        block_process_2_prepare_data,
        block_process_3_evaluate_single_rule,
        block_process_5_build_outputs,
        block_process_6_explain_logic,
        get_block_specs,
        run_block_workflow,
    )
except ModuleNotFoundError:
    from v2.get_RC_value import DEFAULT_BASED_TEMPLATE_PATH, DEFAULT_BASED_TEMPLATE_SHEET, DEFAULT_COREP_DIR  # type: ignore
    from v2.corep_blocks import (  # type: ignore
        block_process_1_extract_values,
        block_process_2_prepare_data,
        block_process_3_evaluate_single_rule,
        block_process_5_build_outputs,
        block_process_6_explain_logic,
        get_block_specs,
        run_block_workflow,
    )


def quick_test() -> List[Dict[str, Any]]:
    frame = pd.DataFrame(
        [
            [None, "0210", "0215"],
            ["0010", 10, 5],
            ["0020", 1, 1],
        ]
    )

    data_mapping = {
        "C07.00": {
            "qx2024": frame,
        }
    }

    rules = [
        pd.Series(
            {
                "Id": "T_EQ_PASS",
                "Templates used": "C07.00",
                "Tables": "",
                "Rows": "0010",
                "Columns": "0210",
                "Sheets": "qx2024",
                "Precondition": None,
                "Formula": "{r0010,c0210} = 10",
                "Arithmetic approach": "exact",
            }
        ),
        pd.Series(
            {
                "Id": "T_EQ_FAIL",
                "Templates used": "C07.00",
                "Tables": "",
                "Rows": "0010",
                "Columns": "0215",
                "Sheets": "qx2024",
                "Precondition": None,
                "Formula": "{r0010,c0215} = 10",
                "Arithmetic approach": "exact",
            }
        ),
        pd.Series(
            {
                "Id": "T_PRECOND",
                "Templates used": "C07.00",
                "Tables": "",
                "Rows": "0020",
                "Columns": "0215",
                "Sheets": "qx2024",
                "Precondition": "{r0020,c0215} > 0",
                "Formula": "{r0020,c0215} = 1",
                "Arithmetic approach": "exact",
            }
        ),
    ]

    results = [
        block_process_3_evaluate_single_rule(rule_row=rule, data_mapping=data_mapping)
        for rule in rules
    ]

    expected = {
        "T_EQ_PASS": "PASS",
        "T_EQ_FAIL": "FAIL",
        "T_PRECOND": "PASS",
    }

    for result in results:
        rule_id = str(result.get("rule_id", ""))
        expected_status = expected.get(rule_id)
        result["expected_status"] = expected_status
        result["self_test_ok"] = result.get("status") == expected_status

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="COREP canonical pipeline (block process flow)")
    parser.add_argument("command", choices=["specs", "extract", "prepare", "run", "explain", "all", "test"])
    parser.add_argument("--config", type=Path, default=DEFAULT_BASED_TEMPLATE_PATH)
    parser.add_argument("--sheet", type=str, default=DEFAULT_BASED_TEMPLATE_SHEET)
    parser.add_argument("--corep-dir", type=Path, default=DEFAULT_COREP_DIR)
    parser.add_argument("--output-dir", type=Path, default=Path("outputs"))
    parser.add_argument("--max-rules", type=int, default=None)
    parser.add_argument("--overwrite-seed", action="store_true")

    parser.add_argument("--templates-used", type=str, default="C07.00")
    parser.add_argument("--tables", type=str, default="C07.00.a")
    parser.add_argument("--rows", type=str, default="0010")
    parser.add_argument("--columns", type=str, default="0210")
    parser.add_argument("--sheets", type=str, default=None)

    args = parser.parse_args()

    if args.command == "specs":
        for spec in get_block_specs():
            print(spec)
        return

    if args.command == "extract":
        print(
            block_process_1_extract_values(
                templates_used=args.templates_used,
                tables=args.tables,
                rows=args.rows,
                columns=args.columns,
                sheets=args.sheets,
                corep_dir=args.corep_dir,
            )
        )
        return

    if args.command == "prepare":
        print(
            block_process_2_prepare_data(
                config_path=args.config,
                sheet_name=args.sheet,
                corep_dir=args.corep_dir,
                overwrite_seed=args.overwrite_seed,
            )
        )
        return

    if args.command == "run":
        print(
            block_process_5_build_outputs(
                config_path=args.config,
                sheet_name=args.sheet,
                corep_dir=args.corep_dir,
                output_dir=args.output_dir,
                max_rules=args.max_rules,
            )
        )
        return

    if args.command == "explain":
        print(
            block_process_6_explain_logic(
                config_path=args.config,
                sheet_name=args.sheet,
                output_path=args.output_dir / "rule_logic_audit.xlsx",
                max_rules=args.max_rules,
            )
        )
        return

    if args.command == "test":
        for item in quick_test():
            print(item)
        return

    print(
        run_block_workflow(
            config_path=args.config,
            sheet_name=args.sheet,
            corep_dir=args.corep_dir,
            output_dir=args.output_dir,
            max_rules=args.max_rules,
            overwrite_seed=args.overwrite_seed,
        )
    )


if __name__ == "__main__":
    main()
