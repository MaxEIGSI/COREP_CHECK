from __future__ import annotations

from dataclasses import dataclass
from itertools import product
from pathlib import Path
import math
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

import openpyxl
import pandas as pd

from src.data.get_RC_value import (
    ALL_SENTINEL,
    DEFAULT_BASED_TEMPLATE_PATH,
    DEFAULT_BASED_TEMPLATE_SHEET,
    DEFAULT_COREP_DIR,
    build_column_code_map,
    build_row_code_map,
    contains_template_hint,
    letter_to_sequence,
    normalize_axis_code,
    parse_selector,
    sequence_to_marker,
    worksheet_to_dataframe,
)


INTERVAL_TOLERANCE = 1e-6


@dataclass(frozen=True)
class Coordinate:
    template: str
    table: str
    sheet: str
    row: Optional[str]
    column: Optional[str]


@dataclass
class RuleDetail:
    coordinates: Tuple[str, str, str, Optional[str], Optional[str]]
    expected: Any
    actual: Any
    passed: bool
    message: str = ""


@dataclass
class RuleResult:
    rule_id: str
    status: str
    details: List[RuleDetail]
    reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "status": self.status,
            "reason": self.reason,
            "details": [
                {
                    "coordinates": d.coordinates,
                    "expected": d.expected,
                    "actual": d.actual,
                    "passed": d.passed,
                    "message": d.message,
                }
                for d in self.details
            ],
        }


@dataclass(frozen=True)
class RefSpec:
    template: Optional[str]
    table: Optional[str]
    row: Optional[str]
    column: Optional[str]
    sheet: Optional[str]
    wildcard: bool
    member_part: bool


@dataclass
class SheetContext:
    dataframe: pd.DataFrame
    row_map: Dict[str, int]
    col_map: Dict[str, Any]


class RuleEngineError(Exception):
    pass


class NonDslRuleError(RuleEngineError):
    pass


def normalize_template_id(template_code: str) -> str:
    code = str(template_code).strip().upper().replace(" ", "")
    if not re.fullmatch(r"[A-Z]\d{2}\.\d{2}", code):
        raise RuleEngineError(f"Invalid template code format: {template_code}")
    return code


def template_to_file_name(template_code: str) -> str:
    code = normalize_template_id(template_code)
    return f"G_EU_C_{code.replace('.', '')}.xlsx"


def template_to_file_path(template_code: str, corep_dir: Path) -> Path:
    path = corep_dir / template_to_file_name(template_code)
    if not path.exists():
        raise RuleEngineError(f"COREP file not found for template {template_code}: {path}")
    return path


def split_table_identifier(table_name: str) -> Tuple[str, Optional[str]]:
    cleaned = str(table_name).strip().upper().replace(" ", "")
    parts = cleaned.split(".")
    if len(parts) >= 3 and parts[-1].isalpha():
        return ".".join(parts[:-1]), parts[-1].lower()
    return cleaned, None


def resolve_sheet_for_table_generic(
    wb: openpyxl.Workbook,
    template_code: str,
    table_name: str,
) -> str:
    table_template, suffix_letter = split_table_identifier(table_name)
    norm_template = normalize_template_id(template_code)
    if normalize_template_id(table_template) != norm_template:
        raise RuleEngineError(f"Table {table_name} does not match template {template_code}")
    if suffix_letter is None:
        raise RuleEngineError(f"Table {table_name} has no suffix letter")

    marker = sequence_to_marker(letter_to_sequence(suffix_letter))
    marker_pattern = re.compile(rf"-\s*{marker}\b")

    for ws in wb.worksheets:
        has_marker = False
        has_template = False
        for row in ws.iter_rows(values_only=True):
            for value in row:
                if value is None:
                    continue
                text = str(value).strip()
                if not text:
                    continue
                if marker_pattern.search(text):
                    has_marker = True
                if contains_template_hint(text, norm_template):
                    has_template = True
                if has_marker and has_template:
                    return ws.title

    raise RuleEngineError(f"No worksheet found for table {table_name} (marker {marker})")


def is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def to_number(value: Any) -> Optional[float]:
    if is_empty(value):
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    text = str(value).strip()
    try:
        return float(text)
    except ValueError:
        return None


def values_sum(values: Iterable[Any]) -> Optional[float]:
    nums = [to_number(v) for v in values]
    nums = [n for n in nums if n is not None]
    if not nums:
        return None
    return float(sum(nums))


def values_count(values: Iterable[Any]) -> int:
    return sum(1 for v in values if not is_empty(v))


def values_max(values: Iterable[Any]) -> Optional[float]:
    nums = [to_number(v) for v in values]
    nums = [n for n in nums if n is not None]
    if not nums:
        return None
    return max(nums)


def values_min(values: Iterable[Any]) -> Optional[float]:
    nums = [to_number(v) for v in values]
    nums = [n for n in nums if n is not None]
    if not nums:
        return None
    return min(nums)


def values_length(value: Any) -> int:
    if is_empty(value):
        return 0
    return len(str(value).strip())


def member_part(value: Any) -> Optional[str]:
    if is_empty(value):
        return None
    text = str(value).strip()
    match = re.search(r"\(([^()]+)\)", text)
    if match:
        return match.group(1)
    if ":" in text:
        return text.split(":", 1)[1]
    return text


def parse_assignment_groups(value: Any, prefix: str) -> Optional[List[Dict[str, str]]]:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    if not text or "{" not in text or "=" not in text:
        return None

    groups = re.findall(r"\{([^{}]+)\}", text)
    parsed_groups: List[Dict[str, str]] = []

    for group in groups:
        mapping: Dict[str, str] = {}
        for raw in group.split(";"):
            token = raw.strip()
            if not token or "=" not in token:
                continue
            key, val = token.split("=", 1)
            key = key.strip()
            val_norm = normalize_axis_code(val.strip())
            if not key.lower().startswith(prefix) or val_norm is None:
                continue
            mapping[key.lower()] = val_norm.zfill(4)
        if mapping:
            parsed_groups.append(mapping)

    return parsed_groups or None


def clean_formula_text(text: str) -> str:
    normalized = " ".join(text.replace("\n", " ").split())
    normalized = re.sub(r"(\S+)\s*!=\s*empty\b", r"is_not_empty(\1)", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"(\S+)\s*=\s*empty\b", r"is_empty_value(\1)", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\bempty\b", "None", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"(?<![<>=!])=(?!=)", "==", normalized)
    return normalized


def rebind_ref_keys(
    expression: str,
    refs: Dict[str, RefSpec],
    prefix: str,
) -> Tuple[str, Dict[str, RefSpec]]:
    rebound: Dict[str, RefSpec] = {}
    updated = expression
    for old_key, ref in refs.items():
        new_key = f"{prefix}{old_key}"
        updated = updated.replace(old_key, new_key)
        rebound[new_key] = ref
    return updated, rebound


def parse_ref_token(raw_ref: str, wildcard: bool) -> RefSpec:
    content = raw_ref.strip()
    member_flag = False
    lowered = content.lower()
    if lowered.startswith("member part of "):
        content = content[15:].strip()
        member_flag = True

    template: Optional[str] = None
    table: Optional[str] = None
    row: Optional[str] = None
    column: Optional[str] = None
    sheet: Optional[str] = None

    for part in [p.strip() for p in content.split(",") if p.strip()]:
        part_low = part.lower()

        if re.fullmatch(r"c\d{2}\.\d{2}\.[a-z]+", part_low):
            template = normalize_template_id(".".join(part.split(".")[:2]))
            table = part.upper()
            continue

        if re.fullmatch(r"c\d{2}\.\d{2}", part_low):
            template = normalize_template_id(part.upper())
            continue

        if part_low.startswith("r"):
            token = part_low[1:]
            axis = normalize_axis_code(token)
            row = axis.zfill(4) if axis else token
            continue

        if part_low.startswith("c") and re.fullmatch(r"c\d{2,4}", part_low):
            token = part_low[1:]
            axis = normalize_axis_code(token)
            column = axis.zfill(4) if axis else token
            continue

        if re.fullmatch(r"qx\d+", part_low):
            sheet = part_low
            continue

        if re.fullmatch(r"s\w+", part_low):
            sheet = part
            continue

    return RefSpec(
        template=template,
        table=table,
        row=row,
        column=column,
        sheet=sheet,
        wildcard=wildcard,
        member_part=member_flag,
    )


class CorepDataRepository:
    def __init__(self, corep_dir: str | Path = DEFAULT_COREP_DIR):
        self.corep_dir = Path(corep_dir)
        self._workbooks: Dict[str, openpyxl.Workbook] = {}
        self._sheet_context: Dict[Tuple[str, str], SheetContext] = {}

    def workbook_for_template(self, template: str) -> openpyxl.Workbook:
        norm_template = normalize_template_id(template)
        if norm_template not in self._workbooks:
            file_path = template_to_file_path(norm_template, self.corep_dir)
            self._workbooks[norm_template] = openpyxl.load_workbook(file_path, data_only=True)
        return self._workbooks[norm_template]

    def get_table_sheet(self, template: str, table: str) -> str:
        wb = self.workbook_for_template(template)
        return resolve_sheet_for_table_generic(wb, template, table)

    def all_sheets(self, template: str) -> List[str]:
        wb = self.workbook_for_template(template)
        return [ws.title for ws in wb.worksheets]

    def context(self, template: str, sheet: str) -> SheetContext:
        key = (normalize_template_id(template), sheet)
        if key not in self._sheet_context:
            wb = self.workbook_for_template(template)
            ws = wb[sheet]
            df = worksheet_to_dataframe(ws)
            row_map = build_row_code_map(df)
            col_map = build_column_code_map(df)
            row_map = {k.zfill(4): v for k, v in row_map.items()}
            col_map = {k.zfill(4): v for k, v in col_map.items()}
            self._sheet_context[key] = SheetContext(df, row_map, col_map)
        return self._sheet_context[key]


class DimensionResolver:
    def __init__(self, repository: CorepDataRepository):
        self.repository = repository

    def _parse_templates(self, value: Any) -> List[str]:
        tokens = parse_selector(value) or []
        return [normalize_template_id(t) for t in tokens if t != ALL_SENTINEL]

    def _parse_tables(self, value: Any, template: str) -> List[str]:
        tokens = parse_selector(value)
        if not tokens:
            return []
        tables = []
        for token in tokens:
            cleaned = str(token).strip().upper()
            if re.fullmatch(r"[A-Z]\d{2}\.\d{2}\.[A-Z]+", cleaned):
                if cleaned.startswith(template):
                    tables.append(cleaned)
        return tables

    def _parse_axis_codes(self, value: Any) -> Optional[List[str]]:
        tokens = parse_selector(value)
        if not tokens:
            return None
        if ALL_SENTINEL in tokens:
            return [ALL_SENTINEL]
        out: List[str] = []
        for token in tokens:
            norm = normalize_axis_code(token)
            if norm is not None:
                out.append(norm.zfill(4))
        return out or None

    def resolve_scope(self, rule_row: pd.Series) -> Dict[str, Any]:
        templates = self._parse_templates(rule_row.get("Templates used"))
        if not templates:
            raise RuleEngineError("Rule has no valid templates")

        rows = self._parse_axis_codes(rule_row.get("Rows"))
        columns = self._parse_axis_codes(rule_row.get("Columns"))
        sheets_selector = parse_selector(rule_row.get("Sheets"))

        row_assignments = parse_assignment_groups(rule_row.get("Rows"), "r")
        col_assignments = parse_assignment_groups(rule_row.get("Columns"), "c")

        template_scope: Dict[str, Dict[str, Any]] = {}

        for template in templates:
            tables = self._parse_tables(rule_row.get("Tables"), template)
            if tables:
                table_to_sheet = {
                    table: self.repository.get_table_sheet(template, table) for table in tables
                }
            else:
                all_sheet_names = self.repository.all_sheets(template)
                if sheets_selector and ALL_SENTINEL not in sheets_selector:
                    selected = [s for s in sheets_selector if s in all_sheet_names]
                    table_to_sheet = {sheet: sheet for sheet in selected}
                else:
                    table_to_sheet = {sheet: sheet for sheet in all_sheet_names}

            template_scope[template] = {
                "table_to_sheet": table_to_sheet,
                "rows": rows,
                "columns": columns,
            }

        return {
            "templates": templates,
            "template_scope": template_scope,
            "row_assignments": row_assignments,
            "col_assignments": col_assignments,
        }


class FormulaParser:
    REF_PATTERN = re.compile(r"\{([^{}]+)\}(\(\*\))?")

    def __init__(self):
        self._refs: Dict[str, RefSpec] = {}

    def parse(self, expression: str) -> Tuple[str, Dict[str, RefSpec]]:
        if not expression or not isinstance(expression, str):
            raise NonDslRuleError("Empty expression")

        text = expression.strip()
        if text.lower().startswith("for each"):
            raise NonDslRuleError("Natural language rule (For each...) is not DSL")

        self._refs = {}
        counter = 0

        def _replace(match: re.Match[str]) -> str:
            nonlocal counter
            raw = match.group(1)
            wildcard = bool(match.group(2))
            key = f"__ref_{counter}__"
            self._refs[key] = parse_ref_token(raw, wildcard)
            counter += 1
            return key

        replaced = self.REF_PATTERN.sub(_replace, text)
        replaced = clean_formula_text(replaced)

        if "For each" in replaced:
            raise NonDslRuleError("Natural language clause not supported")

        return replaced, dict(self._refs)


class ValueResolver:
    def __init__(self, repository: CorepDataRepository, scope: Dict[str, Any]):
        self.repository = repository
        self.scope = scope

    def _resolve_axis(
        self,
        base: Coordinate,
        ref: RefSpec,
        alias_map: Dict[str, str],
        axis: str,
    ) -> Optional[str]:
        token = ref.row if axis == "row" else ref.column
        base_val = base.row if axis == "row" else base.column
        if token is None:
            return base_val
        if token.lower() in alias_map:
            return alias_map[token.lower()].zfill(4)
        norm = normalize_axis_code(token)
        return norm.zfill(4) if norm else token

    def _value_for_coordinate(self, coord: Coordinate) -> Any:
        context = self.repository.context(coord.template, coord.sheet)
        if coord.row is None or coord.column is None:
            return None
        row_idx = context.row_map.get(coord.row)
        col_idx = context.col_map.get(coord.column)
        if row_idx is None or col_idx is None:
            return None
        return context.dataframe.loc[row_idx, col_idx]

    def resolve_ref(self, base: Coordinate, ref: RefSpec, alias_map: Dict[str, str]) -> Any:
        template = ref.template or base.template
        table = ref.table or base.table
        sheet = ref.sheet or base.sheet

        if ref.table and not ref.sheet:
            sheet = self.repository.get_table_sheet(template, table)

        available_sheets = self.repository.all_sheets(template)
        if sheet not in available_sheets:
            sheet = base.sheet

        row = self._resolve_axis(base, ref, alias_map, "row")
        column = self._resolve_axis(base, ref, alias_map, "column")

        if ref.wildcard:
            template_meta = self.scope["template_scope"][template]
            rows = template_meta["rows"]
            cols = template_meta["columns"]

            if rows is None or ALL_SENTINEL in rows:
                rows_iter = list(self.repository.context(template, sheet).row_map.keys())
            else:
                rows_iter = rows

            if cols is None or ALL_SENTINEL in cols:
                cols_iter = list(self.repository.context(template, sheet).col_map.keys())
            else:
                cols_iter = cols

            selected_rows = [row] if row is not None else rows_iter
            selected_cols = [column] if column is not None else cols_iter

            out = []
            for r_code, c_code in product(selected_rows, selected_cols):
                coord = Coordinate(template, table, sheet, r_code, c_code)
                value = self._value_for_coordinate(coord)
                if ref.member_part:
                    value = member_part(value)
                out.append(value)
            return out

        coord = Coordinate(template, table, sheet, row, column)
        value = self._value_for_coordinate(coord)
        return member_part(value) if ref.member_part else value


class RuleEvaluator:
    def __init__(
        self,
        repository: CorepDataRepository,
        tolerance: float = INTERVAL_TOLERANCE,
    ):
        self.repository = repository
        self.tolerance = tolerance
        self.dimension_resolver = DimensionResolver(repository)
        self.formula_parser = FormulaParser()

    def _comparison_env(self, arithmetic_approach: str) -> Dict[str, Any]:
        exact_mode = str(arithmetic_approach).strip().lower() != "interval"

        def cmp_eq(left: Any, right: Any) -> bool:
            left_num = to_number(left)
            right_num = to_number(right)
            if left_num is not None and right_num is not None:
                if exact_mode:
                    return left_num == right_num
                return abs(left_num - right_num) <= self.tolerance
            return left == right

        def cmp_ne(left: Any, right: Any) -> bool:
            return not cmp_eq(left, right)

        def cmp_ge(left: Any, right: Any) -> bool:
            left_num = to_number(left)
            right_num = to_number(right)
            if left_num is not None and right_num is not None:
                if exact_mode:
                    return left_num >= right_num
                return left_num >= right_num - self.tolerance
            return left >= right

        def cmp_gt(left: Any, right: Any) -> bool:
            left_num = to_number(left)
            right_num = to_number(right)
            if left_num is not None and right_num is not None:
                if exact_mode:
                    return left_num > right_num
                return left_num > right_num - self.tolerance
            return left > right

        def cmp_le(left: Any, right: Any) -> bool:
            left_num = to_number(left)
            right_num = to_number(right)
            if left_num is not None and right_num is not None:
                if exact_mode:
                    return left_num <= right_num
                return left_num <= right_num + self.tolerance
            return left <= right

        def cmp_lt(left: Any, right: Any) -> bool:
            left_num = to_number(left)
            right_num = to_number(right)
            if left_num is not None and right_num is not None:
                if exact_mode:
                    return left_num < right_num
                return left_num < right_num + self.tolerance
            return left < right

        return {
            "cmp_eq": cmp_eq,
            "cmp_ne": cmp_ne,
            "cmp_ge": cmp_ge,
            "cmp_gt": cmp_gt,
            "cmp_le": cmp_le,
            "cmp_lt": cmp_lt,
        }

    def _wrap_comparisons(self, expr: str) -> str:
        expr = re.sub(r"(\S+)\s*==\s*(\S+)", r"cmp_eq(\1,\2)", expr)
        expr = re.sub(r"(\S+)\s*!=\s*(\S+)", r"cmp_ne(\1,\2)", expr)
        expr = re.sub(r"(\S+)\s*>=\s*(\S+)", r"cmp_ge(\1,\2)", expr)
        expr = re.sub(r"(\S+)\s*<=\s*(\S+)", r"cmp_le(\1,\2)", expr)
        expr = re.sub(r"(\S+)\s*>\s*(\S+)", r"cmp_gt(\1,\2)", expr)
        expr = re.sub(r"(\S+)\s*<\s*(\S+)", r"cmp_lt(\1,\2)", expr)
        return expr

    def _candidate_axes(
        self,
        template: str,
        table: str,
        sheet: str,
        rows: Optional[List[str]],
        columns: Optional[List[str]],
    ) -> Tuple[List[Optional[str]], List[Optional[str]]]:
        context = self.repository.context(template, sheet)
        row_candidates = rows
        col_candidates = columns

        if row_candidates is None or ALL_SENTINEL in row_candidates:
            row_candidates = list(context.row_map.keys())
        if col_candidates is None or ALL_SENTINEL in col_candidates:
            col_candidates = list(context.col_map.keys())

        if not row_candidates:
            row_candidates = [None]
        if not col_candidates:
            col_candidates = [None]

        return row_candidates, col_candidates

    def _alias_maps(
        self,
        row_assignments: Optional[List[Dict[str, str]]],
        col_assignments: Optional[List[Dict[str, str]]],
    ) -> List[Dict[str, str]]:
        row_groups = row_assignments or [{}]
        col_groups = col_assignments or [{}]
        out: List[Dict[str, str]] = []
        for rg, cg in product(row_groups, col_groups):
            merged = {}
            merged.update(rg)
            merged.update(cg)
            out.append(merged)
        return out

    def evaluate_rule(self, rule_row: pd.Series) -> RuleResult:
        rule_id = str(rule_row.get("Id", "UNKNOWN"))
        formula = rule_row.get("Formula")
        precondition = rule_row.get("Precondition")
        arithmetic = str(rule_row.get("Arithmetic approach", "exact"))

        try:
            scope = self.dimension_resolver.resolve_scope(rule_row)
            value_resolver = ValueResolver(self.repository, scope)
        except Exception as exc:
            return RuleResult(rule_id=rule_id, status="SKIPPED", details=[], reason=str(exc))

        if formula is None or (isinstance(formula, float) and pd.isna(formula)):
            return RuleResult(rule_id=rule_id, status="SKIPPED", details=[], reason="Missing formula")

        try:
            formula_expr, formula_refs = self.formula_parser.parse(str(formula))
            formula_expr, formula_refs = rebind_ref_keys(formula_expr, formula_refs, "f_")
            formula_expr = self._wrap_comparisons(formula_expr)
        except NonDslRuleError as exc:
            return RuleResult(rule_id=rule_id, status="SKIPPED", details=[], reason=str(exc))
        except Exception as exc:
            return RuleResult(rule_id=rule_id, status="SKIPPED", details=[], reason=f"Formula parse error: {exc}")

        pre_expr: Optional[str] = None
        pre_refs: Dict[str, RefSpec] = {}
        if precondition is not None and not (isinstance(precondition, float) and pd.isna(precondition)):
            try:
                pre_expr, pre_refs = self.formula_parser.parse(str(precondition))
                pre_expr, pre_refs = rebind_ref_keys(pre_expr, pre_refs, "p_")
                pre_expr = self._wrap_comparisons(pre_expr)
            except NonDslRuleError:
                pre_expr = None
                pre_refs = {}
            except Exception:
                pre_expr = None
                pre_refs = {}

        details: List[RuleDetail] = []
        any_fail = False
        env_cmp = self._comparison_env(arithmetic)

        alias_maps = self._alias_maps(scope.get("row_assignments"), scope.get("col_assignments"))

        for template in scope["templates"]:
            meta = scope["template_scope"][template]
            rows = meta["rows"]
            cols = meta["columns"]

            for table, sheet in meta["table_to_sheet"].items():
                row_candidates, col_candidates = self._candidate_axes(template, table, sheet, rows, cols)

                for row_code, col_code, alias_map in product(row_candidates, col_candidates, alias_maps):
                    coordinate = Coordinate(template, table, sheet, row_code, col_code)

                    runtime_env: Dict[str, Any] = {
                        "sum": lambda *args: values_sum(_flatten(args)),
                        "count": lambda *args: values_count(_flatten(args)),
                        "max": lambda *args: values_max(_flatten(args)),
                        "min": lambda *args: values_min(_flatten(args)),
                        "length": values_length,
                        "is_empty_value": is_empty,
                        "is_not_empty": lambda x: not is_empty(x),
                        "None": None,
                        **env_cmp,
                    }

                    for key, ref in {**pre_refs, **formula_refs}.items():
                        runtime_env[key] = value_resolver.resolve_ref(coordinate, ref, alias_map)

                    if pre_expr:
                        try:
                            pre_ok = bool(eval(pre_expr, {"__builtins__": {}}, runtime_env))
                        except Exception:
                            pre_ok = False
                        if not pre_ok:
                            continue

                    try:
                        actual = eval(formula_expr, {"__builtins__": {}}, runtime_env)
                        passed = bool(actual)
                        expected = True
                    except Exception as exc:
                        passed = False
                        actual = None
                        expected = True
                        msg = f"Evaluation error: {exc}"
                        details.append(
                            RuleDetail(
                                coordinates=(
                                    coordinate.template,
                                    coordinate.table,
                                    coordinate.sheet,
                                    coordinate.row,
                                    coordinate.column,
                                ),
                                expected=expected,
                                actual=actual,
                                passed=passed,
                                message=msg,
                            )
                        )
                        any_fail = True
                        continue

                    details.append(
                        RuleDetail(
                            coordinates=(
                                coordinate.template,
                                coordinate.table,
                                coordinate.sheet,
                                coordinate.row,
                                coordinate.column,
                            ),
                            expected=expected,
                            actual=actual,
                            passed=passed,
                        )
                    )
                    if not passed:
                        any_fail = True

        if not details:
            return RuleResult(rule_id=rule_id, status="SKIPPED", details=[], reason="No coordinates evaluated")

        return RuleResult(rule_id=rule_id, status="FAIL" if any_fail else "PASS", details=details)


def _flatten(values: Iterable[Any]) -> List[Any]:
    out: List[Any] = []
    for value in values:
        if isinstance(value, list):
            out.extend(value)
        elif isinstance(value, tuple):
            out.extend(list(value))
        else:
            out.append(value)
    return out


def load_rules(
    config_path: str | Path = DEFAULT_BASED_TEMPLATE_PATH,
    sheet_name: str = DEFAULT_BASED_TEMPLATE_SHEET,
) -> pd.DataFrame:
    return pd.read_excel(config_path, sheet_name=sheet_name, header=1)


def evaluate_rules(
    config_path: str | Path = DEFAULT_BASED_TEMPLATE_PATH,
    sheet_name: str = DEFAULT_BASED_TEMPLATE_SHEET,
    corep_dir: str | Path = DEFAULT_COREP_DIR,
    max_rules: Optional[int] = None,
) -> List[Dict[str, Any]]:
    df = load_rules(config_path=config_path, sheet_name=sheet_name)
    repository = CorepDataRepository(corep_dir=corep_dir)
    evaluator = RuleEvaluator(repository)

    results: List[Dict[str, Any]] = []
    iterable = df.iterrows()
    for i, (_, row) in enumerate(iterable, start=1):
        result = evaluator.evaluate_rule(row)
        results.append(result.to_dict())
        if max_rules is not None and i >= max_rules:
            break

    return results


if __name__ == "__main__":
    output = evaluate_rules(max_rules=20)
    summary = pd.Series([item["status"] for item in output]).value_counts().to_dict()
    print("Summary:", summary)
    print("Sample result:")
    print(output[0] if output else {})
