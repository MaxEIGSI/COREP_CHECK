from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

import pandas as pd
from openpyxl.cell.cell import MergedCell

try:
    from v2.get_RC_value import (
        DEFAULT_BASED_TEMPLATE_PATH,
        DEFAULT_BASED_TEMPLATE_SHEET,
        DEFAULT_COREP_DIR,
        normalize_axis_code,
        parse_selector,
    )
    from v2.rule_engine import CorepDataRepository, RuleEngineError, normalize_template_id
except ModuleNotFoundError:
    from v2.get_RC_value import (
        DEFAULT_BASED_TEMPLATE_PATH,
        DEFAULT_BASED_TEMPLATE_SHEET,
        DEFAULT_COREP_DIR,
        normalize_axis_code,
        parse_selector,
    )
    from v2.rule_engine import CorepDataRepository, RuleEngineError, normalize_template_id


def _parse_axis_tokens(value: Any) -> Set[str]:
    tokens = parse_selector(value) or []
    out: Set[str] = set()
    for token in tokens:
        norm = normalize_axis_code(token)
        if norm is not None:
            out.add(norm.zfill(4))
    return out


def _extract_axis_from_dsl(text: Any) -> Dict[str, Set[str]]:
    rows: Set[str] = set()
    cols: Set[str] = set()

    if text is None or (isinstance(text, float) and pd.isna(text)):
        return {"rows": rows, "cols": cols}

    content = str(text)
    for ref in re.findall(r"\{([^{}]+)\}", content):
        for part in [chunk.strip().lower() for chunk in ref.split(",") if chunk.strip()]:
            if re.fullmatch(r"r\d{2,4}", part):
                norm = normalize_axis_code(part[1:])
                if norm is not None:
                    rows.add(norm.zfill(4))
            elif re.fullmatch(r"c\d{2,4}", part):
                norm = normalize_axis_code(part[1:])
                if norm is not None:
                    cols.add(norm.zfill(4))

    return {"rows": rows, "cols": cols}


def _extract_templates(value: Any) -> Set[str]:
    tokens = parse_selector(value) or []
    out: Set[str] = set()
    for token in tokens:
        raw = str(token).strip()
        if not raw:
            continue
        try:
            out.add(normalize_template_id(raw))
        except Exception:
            continue
    return out


def _target_templates_from_rules(rules_df: pd.DataFrame) -> Set[str]:
    templates: Set[str] = set()
    for _, row in rules_df.iterrows():
        templates.update(_extract_templates(row.get("Templates used")))
    return templates


def _targets_for_template(rules_df: pd.DataFrame, template: str) -> Dict[str, Set[str]]:
    row_codes: Set[str] = set()
    col_codes: Set[str] = set()

    for _, rule in rules_df.iterrows():
        templates = _extract_templates(rule.get("Templates used"))
        if template not in templates:
            continue

        row_codes.update(_parse_axis_tokens(rule.get("Rows")))
        col_codes.update(_parse_axis_tokens(rule.get("Columns")))

        dsl_formula = _extract_axis_from_dsl(rule.get("Formula"))
        dsl_pre = _extract_axis_from_dsl(rule.get("Precondition"))

        row_codes.update(dsl_formula["rows"])
        row_codes.update(dsl_pre["rows"])
        col_codes.update(dsl_formula["cols"])
        col_codes.update(dsl_pre["cols"])

    return {"rows": row_codes, "cols": col_codes}


def _deterministic_value(template: str, sheet: str, row_code: str, col_code: str) -> int:
    seed = f"{template}|{sheet}|{row_code}|{col_code}"
    return (sum(ord(ch) for ch in seed) % 97) + 1


def seed_corep_values(
    config_path: Path = DEFAULT_BASED_TEMPLATE_PATH,
    sheet_name: str = DEFAULT_BASED_TEMPLATE_SHEET,
    corep_dir: Path = DEFAULT_COREP_DIR,
    overwrite: bool = False,
) -> Dict[str, int]:
    rules_df = pd.read_excel(config_path, sheet_name=sheet_name, header=1)
    repository = CorepDataRepository(corep_dir=corep_dir)

    templates = _target_templates_from_rules(rules_df)

    updated_cells = 0
    scanned_cells = 0
    touched_workbooks = 0
    missing_templates = 0

    for template in sorted(templates):
        try:
            workbook = repository.workbook_for_template(template)
        except RuleEngineError:
            missing_templates += 1
            continue
        targets = _targets_for_template(rules_df, template)
        if not targets["rows"] or not targets["cols"]:
            continue

        workbook_updated = False
        for sheet in repository.all_sheets(template):
            context = repository.context(template, sheet)
            ws = workbook[sheet]

            available_rows = [code for code in targets["rows"] if code in context.row_map]
            available_cols = [code for code in targets["cols"] if code in context.col_map]

            for row_code in available_rows:
                row_idx = context.row_map[row_code]
                for col_code in available_cols:
                    col_key = context.col_map[col_code]
                    col_idx = context.dataframe.columns.get_loc(col_key)
                    if not isinstance(row_idx, int) or not isinstance(col_idx, int):
                        continue

                    cell = ws.cell(row=row_idx + 1, column=col_idx + 1)
                    if isinstance(cell, MergedCell):
                        continue
                    scanned_cells += 1

                    if not overwrite and cell.value not in (None, ""):
                        continue

                    cell.value = _deterministic_value(template, sheet, row_code, col_code)
                    updated_cells += 1
                    workbook_updated = True

        if workbook_updated:
            workbook.save(corep_dir / f"G_EU_C_{template.replace('.', '')}.xlsx")
            touched_workbooks += 1

    return {
        "updated_cells": updated_cells,
        "scanned_cells": scanned_cells,
        "touched_workbooks": touched_workbooks,
        "templates": len(templates),
        "missing_templates": missing_templates,
    }


def main() -> None:
    # ── PARAMETERS – edit here, then run ──────────────────────────────
    CONFIG    = DEFAULT_BASED_TEMPLATE_PATH
    SHEET     = DEFAULT_BASED_TEMPLATE_SHEET
    COREP_DIR = DEFAULT_COREP_DIR
    OVERWRITE = False
    # ──────────────────────────────────────────────────────────────────

    summary = seed_corep_values(
        config_path=CONFIG,
        sheet_name=SHEET,
        corep_dir=COREP_DIR,
        overwrite=OVERWRITE,
    )
    print(summary)


if __name__ == "__main__":
    main()
