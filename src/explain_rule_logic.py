from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

try:
    from src.get_RC_value import DEFAULT_BASED_TEMPLATE_PATH, DEFAULT_BASED_TEMPLATE_SHEET, parse_selector
except ModuleNotFoundError:
    from get_RC_value import DEFAULT_BASED_TEMPLATE_PATH, DEFAULT_BASED_TEMPLATE_SHEET, parse_selector  # type: ignore


REF_PATTERN = re.compile(r"\{([^{}]+)\}")
ALIAS_PATTERN = re.compile(r"\br([A-Za-z][A-Za-z0-9_]*)\b")
COMPARISON_SPLIT = re.compile(r"(?<![<>!=])=(?!=)")


def _clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def _parse_templates(value: Any) -> List[str]:
    return [token.strip() for token in (parse_selector(value) or []) if token.strip()]


def _parse_tables(value: Any, templates: Sequence[str]) -> List[str]:
    tokens = [token.strip() for token in (parse_selector(value) or []) if token.strip()]
    if tokens:
        return tokens
    return list(templates)


def _parse_mapping_groups(rows_value: Any) -> List[Dict[str, str]]:
    text = _clean(rows_value)
    if not text:
        return []

    groups: List[Dict[str, str]] = []
    for group in re.findall(r"\{([^{}]+)\}", text):
        mapping: Dict[str, str] = {}
        for part in group.split(";"):
            token = part.strip()
            if "=" not in token:
                continue
            key, val = token.split("=", 1)
            key = key.strip()
            val = re.sub(r"\D", "", val.strip())
            if key and val:
                mapping[key] = val.zfill(4)
        if mapping:
            groups.append(mapping)

    return groups


def _extract_aliases(text: str) -> List[str]:
    aliases = sorted(set(ALIAS_PATTERN.findall(text)), key=lambda x: x.lower())
    return [f"r{alias}" for alias in aliases]


def _split_formula_lhs_rhs(formula: str) -> Tuple[str, str]:
    if not formula:
        return "", ""
    match = COMPARISON_SPLIT.search(formula)
    if match:
        return formula[: match.start()].strip(), formula[match.end() :].strip()

    for op in ["!=", ">=", "<=", ">", "<"]:
        idx = formula.find(op)
        if idx >= 0:
            return formula[:idx].strip(), formula[idx + len(op) :].strip()

    return formula.strip(), ""


def _classify_formula_type(formula: str) -> str:
    f = formula.lower()
    if any(token in f for token in ["sum[", "count[", "max[", "min[", "sum(", "count(", "max(", "min("]):
        return "aggregation"
    if " in (" in f or " not in (" in f:
        return "validation"
    if any(token in f for token in [" and ", " or "]):
        return "logical condition"
    if any(token in f for token in [">=", "<=", ">", "<", "!="]):
        return "inequality"
    if "=" in f:
        return "equality"
    return "validation"


def _refs_to_plain(text: str) -> str:
    if not text:
        return ""

    def repl(match: re.Match[str]) -> str:
        body = match.group(1)
        parts = [part.strip() for part in body.split(",") if part.strip()]
        template: Optional[str] = None
        table: Optional[str] = None
        row: Optional[str] = None
        col: Optional[str] = None
        sheet: Optional[str] = None

        for part in parts:
            low = part.lower()
            up = part.upper()
            if re.fullmatch(r"[A-Z]\d{2}\.\d{2}\.[A-Z0-9]+", up):
                table = up
            elif re.fullmatch(r"[A-Z]\d{2}\.\d{2}", up):
                template = up
            elif re.fullmatch(r"r\d{2,4}", low) or re.fullmatch(r"r[A-Za-z][A-Za-z0-9_]*", part):
                row = part
            elif re.fullmatch(r"c\d{2,4}", low):
                col = part
            elif re.fullmatch(r"qx\d+", low) or low.startswith("s"):
                sheet = part

        dims = [
            f"template={template}" if template else None,
            f"table={table}" if table else None,
            f"row={row}" if row else None,
            f"column={col}" if col else None,
            f"sheet={sheet}" if sheet else None,
        ]
        dims = [d for d in dims if d]
        return "cell(" + ", ".join(dims) + ")"

    plain = REF_PATTERN.sub(repl, text)
    plain = plain.replace("!= empty", "is not empty")
    plain = re.sub(r"\bempty\b", "empty", plain, flags=re.IGNORECASE)
    plain = plain.replace("(*)", " (across selected scope)")
    return " ".join(plain.split())


def _resolve_aliases(text: str, mapping: Dict[str, str]) -> str:
    out = text
    for alias, code in mapping.items():
        out = re.sub(rf"\b{re.escape(alias)}\b", f"{alias}[{code}]", out)
    return out


def _business_rule(precondition: str, formula: str) -> str:
    if precondition:
        return f"If {precondition}, then {formula}."
    return f"Validate that {formula}."


def _excel_style(precondition: str, formula: str) -> str:
    if precondition:
        return f"=IF({precondition}, {formula}, TRUE)"
    return f"={formula}"


def _sql_style(precondition: str, formula: str) -> str:
    if precondition:
        return f"CASE WHEN {precondition} THEN ({formula}) ELSE TRUE END"
    return f"({formula})"


def build_logic_audit(
    config_path: Path = DEFAULT_BASED_TEMPLATE_PATH,
    sheet_name: str = DEFAULT_BASED_TEMPLATE_SHEET,
    output_path: Path = Path("outputs/rule_logic_audit.xlsx"),
    max_rules: Optional[int] = None,
) -> Dict[str, Any]:
    df = pd.read_excel(config_path, sheet_name=sheet_name, header=1)
    if max_rules is not None:
        df = df.head(max_rules)

    summary_rows: List[Dict[str, Any]] = []
    mapping_rows: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        rule_id = _clean(row.get("Id")) or "UNKNOWN"
        templates = _parse_templates(row.get("Templates used"))
        tables = _parse_tables(row.get("Tables"), templates)
        pre = _clean(row.get("Precondition"))
        formula = _clean(row.get("Formula"))

        formula_type = _classify_formula_type(formula)
        pre_plain = _refs_to_plain(pre)
        formula_plain = _refs_to_plain(formula)
        business = _business_rule(pre_plain, formula_plain)
        excel_logic = _excel_style(pre_plain, formula_plain)
        sql_logic = _sql_style(pre_plain, formula_plain)

        summary_rows.append(
            {
                "Rule Id": rule_id,
                "Templates": "; ".join(templates),
                "Tables": "; ".join(tables),
                "Formula type": formula_type,
                "Precondition (plain)": pre_plain,
                "Formula (plain)": formula_plain,
                "Business rule": business,
                "Excel-style logic": excel_logic,
                "SQL-style logic": sql_logic,
            }
        )

        mapping_groups = _parse_mapping_groups(row.get("Rows"))
        lhs, rhs = _split_formula_lhs_rhs(formula)
        lhs_aliases = _extract_aliases(lhs)
        rhs_aliases = _extract_aliases(rhs)
        pre_aliases = _extract_aliases(pre)

        if not mapping_groups:
            mapping_rows.append(
                {
                    "Rule Id": rule_id,
                    "Mapping index": 1,
                    "Row mapping": "",
                    "Source rows": ", ".join(pre_aliases + rhs_aliases),
                    "Target rows": ", ".join(lhs_aliases),
                    "Precondition (resolved)": pre_plain,
                    "Formula (resolved)": formula_plain,
                }
            )
            continue

        for idx, mapping in enumerate(mapping_groups, start=1):
            row_mapping_text = "; ".join([f"{k}={v}" for k, v in mapping.items()])

            resolved_pre = _resolve_aliases(pre_plain, mapping)
            resolved_formula = _resolve_aliases(formula_plain, mapping)

            source_rows = [f"{alias}={mapping.get(alias, '?')}" for alias in sorted(set(pre_aliases + rhs_aliases))]
            target_rows = [f"{alias}={mapping.get(alias, '?')}" for alias in sorted(set(lhs_aliases))]

            mapping_rows.append(
                {
                    "Rule Id": rule_id,
                    "Mapping index": idx,
                    "Row mapping": row_mapping_text,
                    "Source rows": "; ".join(source_rows),
                    "Target rows": "; ".join(target_rows),
                    "Precondition (resolved)": resolved_pre,
                    "Formula (resolved)": resolved_formula,
                }
            )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_df = pd.DataFrame(summary_rows)
    mapping_df = pd.DataFrame(mapping_rows)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="Rule Logic", index=False)
        mapping_df.to_excel(writer, sheet_name="Row Mappings", index=False)

        for sheet_name_key in ["Rule Logic", "Row Mappings"]:
            ws = writer.sheets[sheet_name_key]
            for col_cells in ws.columns:
                max_len = max((len(str(cell.value)) for cell in col_cells if cell.value is not None), default=8)
                ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 2, 80)

    return {
        "rules_processed": len(summary_df),
        "mappings_processed": len(mapping_df),
        "output_path": str(output_path),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a concise logic-audit workbook from config rules")
    parser.add_argument("--config", type=Path, default=DEFAULT_BASED_TEMPLATE_PATH)
    parser.add_argument("--sheet", type=str, default=DEFAULT_BASED_TEMPLATE_SHEET)
    parser.add_argument("--output", type=Path, default=Path("outputs/rule_logic_audit.xlsx"))
    parser.add_argument("--max-rules", type=int, default=None)
    args = parser.parse_args()

    result = build_logic_audit(
        config_path=args.config,
        sheet_name=args.sheet,
        output_path=args.output,
        max_rules=args.max_rules,
    )
    print(result)


if __name__ == "__main__":
    main()
