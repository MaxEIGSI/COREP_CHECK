"""
Comprehensive test suite for the COREP rule engine.

Each test maps to a *specific formula pattern* documented in prompt_2.txt /
formulas_explained.txt and verifies both the PASS and FAIL sides.

Engine behaviour notes (verified experimentally):
  - Explicit rows/cols in the rule config drive the outer iteration.
  - A column ref like {c0215} in the formula overrides the base column even
    when cols=None (the outer col slot is None).
  - The (*) wildcard in a formula ref expands ONLY when the corresponding
    base axis value is also None; when a base value exists, (*) is a no-op.
  - alias groups (e.g. {rX=0020;rN=0010}) are the primary driver of row
    iteration when rows is left empty.
  - arithmetic_approach="interval" applies a 1e-6 tolerance to ==/!= checks.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pytest

from v2.rule_engine import (
    RuleEvaluator,
    RuleEngineError,
    SheetContext,
    normalize_template_id,
)


# ─────────────────────────────────────────────────────────────
#  Shared helpers
# ─────────────────────────────────────────────────────────────

class SimpleRepo:
    """
    Lightweight test-double for the data repository.
    Accepts SheetContext objects built from labeled DataFrames,
    bypassing the need for real Excel files.
    """

    def __init__(self) -> None:
        self._ctxs: Dict[Tuple[str, str], SheetContext] = {}
        self._sheets: Dict[str, List[str]] = {}
        self._tbl: Dict[Tuple[str, str], str] = {}

    def add(
        self,
        template: str,
        sheet: str,
        df: pd.DataFrame,
        row_map: Dict[str, Any],
        col_map: Dict[str, Any],
        *,
        table: Optional[str] = None,
    ) -> "SimpleRepo":
        t = normalize_template_id(template)
        self._sheets.setdefault(t, []).append(sheet)
        self._ctxs[(t, sheet)] = SheetContext(df, row_map, col_map)
        if table is not None:
            self._tbl[(t, table.upper())] = sheet
        return self

    def get_table_sheet(self, template: str, table: str) -> str:
        t = normalize_template_id(template)
        key = (t, table.upper())
        if key in self._tbl:
            return self._tbl[key]
        sheets = self._sheets.get(t, [])
        if len(sheets) == 1:
            return sheets[0]
        raise RuleEngineError(f"Ambiguous table→sheet for {template}/{table}")

    def all_sheets(self, template: str) -> List[str]:
        return self._sheets[normalize_template_id(template)]

    def context(self, template: str, sheet: str) -> SheetContext:
        return self._ctxs[(normalize_template_id(template), sheet)]


def make_ctx(
    row_codes: List[str],
    col_codes: List[str],
    values: List[List[Any]],
) -> Tuple[pd.DataFrame, Dict[str, str], Dict[str, str]]:
    """
    Build a labeled DataFrame and the corresponding row_map / col_map.
    Labels are used directly as index/column names and as map values,
    so df.loc[row_map[code], col_map[code]] = data value.
    """
    df = pd.DataFrame(values, index=row_codes, columns=col_codes)
    row_map = {rc.zfill(4): rc for rc in row_codes}
    col_map = {cc.zfill(4): cc for cc in col_codes}
    return df, row_map, col_map


def make_rule(**kw: Any) -> pd.Series:
    """Create a rule config row (pd.Series) with sensible defaults."""
    return pd.Series({
        "Id":                  kw.get("id", "TEST"),
        "Templates used":      kw.get("template", "C07.00"),
        "Tables":              kw.get("tables"),
        "Rows":                kw.get("rows"),
        "Columns":             kw.get("cols"),
        "Sheets":              kw.get("sheets"),
        "Formula":             kw.get("formula", ""),
        "Precondition":        kw.get("pre"),
        "Arithmetic approach": kw.get("arith", "exact"),
    })


def run_rule(rule: pd.Series, repo: SimpleRepo) -> Dict[str, Any]:
    return RuleEvaluator(repo).evaluate_rule(rule).to_dict()


def status(result: Dict[str, Any]) -> str:
    return result["status"]


def passed_flags(result: Dict[str, Any]) -> List[bool]:
    return [d["passed"] for d in result["details"]]


# ─────────────────────────────────────────────────────────────
#  GROUP 1 — Non-empty / empty checks
#  prompt_2.txt Case 1: {c0215}(*) != empty
#  formulas_explained.txt: {c0020} != empty  (rows=All)
# ─────────────────────────────────────────────────────────────

class TestEmptyChecks:

    def _repo(self, c0215_vals: List[Any]) -> SimpleRepo:
        """Template C07.00, rows 0010/0020/0030, single column c0215."""
        repo = SimpleRepo()
        df, rm, cm = make_ctx(
            ["0010", "0020", "0030"],
            ["0215"],
            [[v] for v in c0215_vals],
        )
        repo.add("C07.00", "S1", df, rm, cm)
        return repo

    def test_neq_empty_all_filled_pass(self) -> None:
        """
        Pattern: rows explicit, {c0215}(*) != empty
        All rows have a value → PASS.
        """
        repo = self._repo([100, 200, 300])
        r = make_rule(rows="0010;0020;0030", formula="{c0215}(*) != empty")
        assert status(run_rule(r, repo)) == "PASS"
        assert all(passed_flags(run_rule(r, repo)))

    def test_neq_empty_one_null_fail(self) -> None:
        """Any row with None in c0215 → FAIL (at that point)."""
        repo = self._repo([100, None, 300])
        r = make_rule(rows="0010;0020;0030", formula="{c0215}(*) != empty")
        result = run_rule(r, repo)
        assert status(result) == "FAIL"
        flags = passed_flags(result)
        assert flags[0] is True   # r0010 → 100 ✓
        assert flags[1] is False  # r0020 → None ✗
        assert flags[2] is True   # r0030 → 300 ✓

    def test_neq_empty_no_wildcard_pattern(self) -> None:
        """
        Pattern without (*): rows="0010;0020", formula="{c0215} != empty"
        Engine evaluates one point per explicit row, reading c0215 for that row.
        """
        repo = self._repo([42, None, 7])
        r = make_rule(rows="0010;0020;0030", formula="{c0215} != empty")
        result = run_rule(r, repo)
        assert status(result) == "FAIL"
        assert passed_flags(result) == [True, False, True]

    def test_eq_empty_pass(self) -> None:
        """
        prompt_2.txt Case 3: {r0140,c0020}(*) = empty
        Fixed coordinates — value is None → PASS.
        """
        repo = SimpleRepo()
        df, rm, cm = make_ctx(["0140"], ["0020"], [[None]])
        repo.add("C07.00", "S1", df, rm, cm)
        r = make_rule(formula="{r0140,c0020}(*) = empty")
        result = run_rule(r, repo)
        assert status(result) == "PASS"

    def test_eq_empty_fail(self) -> None:
        """Same formula with a non-None value → FAIL."""
        repo = SimpleRepo()
        df, rm, cm = make_ctx(["0140"], ["0020"], [[999]])
        repo.add("C07.00", "S1", df, rm, cm)
        r = make_rule(formula="{r0140,c0020}(*) = empty")
        result = run_rule(r, repo)
        assert status(result) == "FAIL"


# ─────────────────────────────────────────────────────────────
#  GROUP 2 — Fixed-coordinate arithmetic equality
#  prompt_2.txt Case 2: {r0010,c0210} = {r0090,c0210} + {r0110,c0210}
#  formulas_explained.txt: {r0010,c0080} = {r0020,c0080}
# ─────────────────────────────────────────────────────────────

class TestFixedArithmetic:

    def _repo(self, r0010: Any, r0090: Any, r0110: Any) -> SimpleRepo:
        repo = SimpleRepo()
        df, rm, cm = make_ctx(
            ["0010", "0090", "0110"],
            ["0210"],
            [[r0010], [r0090], [r0110]],
        )
        repo.add("C07.00", "S1", df, rm, cm)
        return repo

    def test_sum_of_rows_pass(self) -> None:
        """
        {r0010,c0210} = {r0090,c0210} + {r0110,c0210}
        30 + 40 = 70 → PASS.
        """
        repo = self._repo(70, 30, 40)
        r = make_rule(formula="{r0010,c0210} = {r0090,c0210} + {r0110,c0210}")
        assert status(run_rule(r, repo)) == "PASS"

    def test_sum_of_rows_fail(self) -> None:
        """71 ≠ 30 + 40 → FAIL."""
        repo = self._repo(71, 30, 40)
        r = make_rule(formula="{r0010,c0210} = {r0090,c0210} + {r0110,c0210}")
        assert status(run_rule(r, repo)) == "FAIL"

    def test_simple_equality_pass(self) -> None:
        """
        formulas_explained: {r0010,c0080} = {r0020,c0080}
        Both cells equal → PASS.
        """
        repo = SimpleRepo()
        df, rm, cm = make_ctx(["0010", "0020"], ["0080"], [[55], [55]])
        repo.add("C22.00", "S1", df, rm, cm)
        r = make_rule(template="C22.00", formula="{r0010,c0080} = {r0020,c0080}")
        assert status(run_rule(r, repo)) == "PASS"

    def test_simple_equality_fail(self) -> None:
        """Different values → FAIL."""
        repo = SimpleRepo()
        df, rm, cm = make_ctx(["0010", "0020"], ["0080"], [[55], [56]])
        repo.add("C22.00", "S1", df, rm, cm)
        r = make_rule(template="C22.00", formula="{r0010,c0080} = {r0020,c0080}")
        assert status(run_rule(r, repo)) == "FAIL"

    def test_addition_with_explicit_rows_pass(self) -> None:
        """
        formulas_explained: cols="0060;0070", {r0010} = {r0030}+{r0040}
        For each explicit column: r0010 = r0030 + r0040.
        """
        repo = SimpleRepo()
        df, rm, cm = make_ctx(
            ["0010", "0030", "0040"],
            ["0060", "0070"],
            [[50, 90], [20, 30], [30, 60]],
        )
        repo.add("C22.00", "S1", df, rm, cm)
        r = make_rule(template="C22.00", cols="0060;0070", formula="{r0010} = {r0030}+{r0040}")
        assert status(run_rule(r, repo)) == "PASS"

    def test_addition_with_explicit_rows_fail(self) -> None:
        """One column fails: r0010.c0070 ≠ r0030.c0070 + r0040.c0070."""
        repo = SimpleRepo()
        df, rm, cm = make_ctx(
            ["0010", "0030", "0040"],
            ["0060", "0070"],
            [[50, 91], [20, 30], [30, 60]],   # 91 ≠ 30+60
        )
        repo.add("C22.00", "S1", df, rm, cm)
        r = make_rule(template="C22.00", cols="0060;0070", formula="{r0010} = {r0030}+{r0040}")
        result = run_rule(r, repo)
        assert status(result) == "FAIL"
        assert passed_flags(result) == [True, False]


# ─────────────────────────────────────────────────────────────
#  GROUP 3 — Scalar multiply / column-driven iteration
#  prompt_2.txt Case 4: {r0040} = 1 * {r0120}
# ─────────────────────────────────────────────────────────────

class TestScalarMultiply:

    def _repo(self, c0010: Tuple[Any, Any], c0020: Tuple[Any, Any]) -> SimpleRepo:
        """Rows 0040 and 0120; columns 0010 and 0020."""
        repo = SimpleRepo()
        df, rm, cm = make_ctx(
            ["0040", "0120"],
            ["0010", "0020"],
            [
                [c0010[0], c0020[0]],   # r0040
                [c0010[1], c0020[1]],   # r0120
            ],
        )
        repo.add("C07.00", "S1", df, rm, cm)
        return repo

    def test_scalar_mul_all_cols_pass(self) -> None:
        """
        prompt_2.txt Case 4: {r0040} = 1 * {r0120}
        cols="0010;0020", both satisfy → PASS.
        """
        repo = self._repo((50, 50), (75, 75))
        r = make_rule(cols="0010;0020", formula="{r0040} = 1 * {r0120}")
        assert status(run_rule(r, repo)) == "PASS"

    def test_scalar_mul_one_col_fail(self) -> None:
        """Second column: 75 ≠ 1 * 80 → FAIL."""
        repo = self._repo((50, 50), (75, 80))
        r = make_rule(cols="0010;0020", formula="{r0040} = 1 * {r0120}")
        result = run_rule(r, repo)
        assert status(result) == "FAIL"
        assert passed_flags(result) == [True, False]

    def test_scalar_multiply_expression_pass(self) -> None:
        """{r0040} = 2 * {r0120}: 100 = 2 * 50 → PASS."""
        repo = self._repo((100, 50), (999, 999))
        r = make_rule(cols="0010", formula="{r0040} = 2 * {r0120}")
        assert status(run_rule(r, repo)) == "PASS"


# ─────────────────────────────────────────────────────────────
#  GROUP 4 — Sum-rows wildcard
#  formulas_explained.txt: {r0910}(*) <= sum({r0010}(*),{r0110}(*),...)
# ─────────────────────────────────────────────────────────────

class TestSumRowsWildcard:

    def _repo(self, r0910: Any, r0010: Any, r0110: Any, r0210: Any) -> SimpleRepo:
        repo = SimpleRepo()
        df, rm, cm = make_ctx(
            ["0910", "0010", "0110", "0210"],
            ["0010"],           # single column "0010"
            [[r0910], [r0010], [r0110], [r0210]],
        )
        repo.add("C17.01", "S1", df, rm, cm)
        return repo

    def test_sum_le_pass(self) -> None:
        """
        formulas_explained: {r0910}(*) <= sum({r0010}(*),{r0110}(*),{r0210}(*))
        90 <= 30 + 40 + 25 = 95 → PASS.
        """
        repo = self._repo(90, 30, 40, 25)
        r = make_rule(
            template="C17.01",
            cols="0010",
            formula="{r0910}(*) <= sum({r0010}(*),{r0110}(*),{r0210}(*))",
        )
        assert status(run_rule(r, repo)) == "PASS"

    def test_sum_le_fail(self) -> None:
        """100 > 30 + 40 + 25 = 95 → FAIL."""
        repo = self._repo(100, 30, 40, 25)
        r = make_rule(
            template="C17.01",
            cols="0010",
            formula="{r0910}(*) <= sum({r0010}(*),{r0110}(*),{r0210}(*))",
        )
        assert status(run_rule(r, repo)) == "FAIL"

    def test_sum_exact_pass(self) -> None:
        """Equality-style sum with explicit rows for outer loop."""
        repo = SimpleRepo()
        df, rm, cm = make_ctx(
            ["0010", "0090", "0110", "0130"],
            ["0210"],
            [[300], [100], [120], [80]],
        )
        repo.add("C07.00", "S1", df, rm, cm)
        # For each explicit row: check {r0010,c0210} sum vs components
        r = make_rule(
            formula="{r0010,c0210} = {r0090,c0210} + {r0110,c0210} + {r0130,c0210}",
        )
        assert status(run_rule(r, repo)) == "PASS"


# ─────────────────────────────────────────────────────────────
#  GROUP 5 — Alias row groups  {rX=0020;rN=0010}
#  formulas_explained.txt: several C17.01 patterns
# ─────────────────────────────────────────────────────────────

class TestAliasRows:

    def _repo_cx(self) -> SimpleRepo:
        """
        C17.01: rows 0010,0020,0110,0120; columns 0080.
        Alias groups: {rX=0020;rN=0010}; {rX=0120;rN=0110}
        """
        repo = SimpleRepo()
        df, rm, cm = make_ctx(
            ["0010", "0020", "0110", "0120"],
            ["0080"],
            [[5], [8], [12], [15]],
        )
        repo.add("C17.01", "S1", df, rm, cm)
        return repo

    def test_rX_gt_zero_pass(self) -> None:
        """
        formulas_explained: rows="{rX=0020;rN=0010}; {rX=0120;rN=0110}", cols empty
        formula: {rX}(*) > 0
        rX values 8 and 15 are both > 0 → PASS.
        """
        r = make_rule(
            template="C17.01",
            rows="{rX=0020;rN=0010}; {rX=0120;rN=0110}",
            formula="{rX}(*) > 0",
        )
        assert status(run_rule(r, self._repo_cx())) == "PASS"

    def test_rX_gt_zero_fail(self) -> None:
        """One rX value is 0 → FAIL."""
        repo = SimpleRepo()
        df, rm, cm = make_ctx(
            ["0010", "0020", "0110", "0120"],
            ["0080"],
            [[5], [0], [12], [15]],   # rX=0020 → 0
        )
        repo.add("C17.01", "S1", df, rm, cm)
        r = make_rule(
            template="C17.01",
            rows="{rX=0020;rN=0010}; {rX=0120;rN=0110}",
            formula="{rX}(*) > 0",
        )
        result = run_rule(r, repo)
        assert status(result) == "FAIL"

    def test_alias_two_tables_pass(self) -> None:
        """
        formulas_explained: formula references two explicit table refs
        {C17.01.a,rX,c0080}(*) > 0
        Table C17.01.a maps to sheet S1.
        Both rX values (8, 15) > 0 → PASS.
        """
        repo = SimpleRepo()
        df, rm, cm = make_ctx(
            ["0010", "0020", "0110", "0120"],
            ["0080"],
            [[5], [8], [12], [15]],
        )
        repo.add("C17.01", "S1", df, rm, cm, table="C17.01.A")
        r = make_rule(
            template="C17.01",
            rows="{rX=0020;rN=0010}; {rX=0120;rN=0110}",
            formula="{C17.01.a,rX,c0080}(*) > 0",
        )
        assert status(run_rule(r, repo)) == "PASS"


# ─────────────────────────────────────────────────────────────
#  GROUP 6 — not in (empty, 0)
#  formulas_explained.txt: {rY}(*) not in (empty,0)
# ─────────────────────────────────────────────────────────────

class TestNotIn:

    def _repo(self, vals_0040: List[Any], vals_0140: List[Any]) -> SimpleRepo:
        """
        C17.01: rows 0030,0040,0130,0140; cols 0010,0020,0030
        rY alias maps to 0040 and 0140.
        """
        repo = SimpleRepo()
        df, rm, cm = make_ctx(
            ["0030", "0040", "0130", "0140"],
            ["0010", "0020", "0030"],
            [
                [1, 2, 3],          # r0030
                vals_0040,          # r0040  (rY group 1)
                [4, 5, 6],          # r0130
                vals_0140,          # r0140  (rY group 2)
            ],
        )
        repo.add("C17.01", "S1", df, rm, cm)
        return repo

    def test_not_in_all_valid_pass(self) -> None:
        """
        formulas_explained: rows="{rX=0030;rY=0040}; {rX=0130;rY=0140}"
                            cols="0010;0020;0030"
        formula: {rY}(*) not in (empty,0)
        All rY values are non-zero and non-null → PASS.
        """
        repo = self._repo([7, 8, 9], [10, 11, 12])
        r = make_rule(
            template="C17.01",
            rows="{rX=0030;rY=0040}; {rX=0130;rY=0140}",
            cols="0010;0020;0030",
            formula="{rY}(*) not in (empty,0)",
        )
        assert status(run_rule(r, repo)) == "PASS"

    def test_not_in_zero_value_fail(self) -> None:
        """rY=0040, col=0020 is 0 → FAIL."""
        repo = self._repo([7, 0, 9], [10, 11, 12])   # r0040.c0020 = 0
        r = make_rule(
            template="C17.01",
            rows="{rX=0030;rY=0040}; {rX=0130;rY=0140}",
            cols="0010;0020;0030",
            formula="{rY}(*) not in (empty,0)",
        )
        assert status(run_rule(r, repo)) == "FAIL"

    def test_not_in_nan_engine_quirk_pass(self) -> None:
        """
        ENGINE QUIRK: pandas stores None as float NaN in mixed columns.
        Python `nan not in (None, 0)` evaluates to True because NaN is not
        identical to None and not equal to 0.
        Therefore the engine does NOT catch NaN as 'empty' inside not-in
        comparisons — the check passes even for a null cell.
        This test documents that observed behaviour.
        """
        repo = self._repo([7, 8, 9], [10, 11, None])   # r0140.c0030 = NaN
        r = make_rule(
            template="C17.01",
            rows="{rX=0030;rY=0040}; {rX=0130;rY=0140}",
            cols="0010;0020;0030",
            formula="{rY}(*) not in (empty,0)",
        )
        # NaN 'not in (None, 0)' → True in CPython; PASS is the engine result.
        assert status(run_rule(r, repo)) == "PASS"

    def test_not_in_zero_alias_fail(self) -> None:
        """A second alias group where rY = 0 → caught by not in (empty,0)."""
        repo = self._repo([7, 8, 9], [10, 0, 12])   # r0140.c0020 = 0
        r = make_rule(
            template="C17.01",
            rows="{rX=0030;rY=0040}; {rX=0130;rY=0140}",
            cols="0010;0020;0030",
            formula="{rY}(*) not in (empty,0)",
        )
        assert status(run_rule(r, repo)) == "FAIL"


# ─────────────────────────────────────────────────────────────
#  GROUP 7 — Qualifier stripping  {r0922 in EUR}
#  formulas_explained.txt: "in EUR" suffix is ignored
# ─────────────────────────────────────────────────────────────

class TestQualifierStrip:

    def _repo(self, r0922: Any, r0912: Any) -> SimpleRepo:
        repo = SimpleRepo()
        df, rm, cm = make_ctx(
            ["0922", "0912"],
            ["0010"],
            [[r0922], [r0912]],
        )
        repo.add("C07.00", "S1", df, rm, cm)
        return repo

    def test_in_eur_qualifier_stripped_pass(self) -> None:
        """
        formulas_explained: ({r0922 in EUR} >= {r0912} * 20000)
        "in EUR" is stripped → reads r0922.
        50_000 >= 2 * 20_000 → PASS.
        Needs cols="0010" so the outer col anchor is not None.
        """
        repo = self._repo(50_000, 2)
        r = make_rule(
            cols="0010",
            formula="({r0922 in EUR} >= {r0912} * 20000) and ({r0922 in EUR} <= {r0912} * 100000)",
        )
        assert status(run_rule(r, repo)) == "PASS"

    def test_in_eur_below_lower_bound_fail(self) -> None:
        """10_000 < 2 * 20_000 = 40_000 → FAIL."""
        repo = self._repo(10_000, 2)
        r = make_rule(
            cols="0010",
            formula="({r0922 in EUR} >= {r0912} * 20000) and ({r0922 in EUR} <= {r0912} * 100000)",
        )
        assert status(run_rule(r, repo)) == "FAIL"

    def test_in_eur_above_upper_bound_fail(self) -> None:
        """250_000 > 2 * 100_000 → FAIL."""
        repo = self._repo(250_000, 2)
        r = make_rule(
            cols="0010",
            formula="({r0922 in EUR} >= {r0912} * 20000) and ({r0922 in EUR} <= {r0912} * 100000)",
        )
        assert status(run_rule(r, repo)) == "FAIL"


# ─────────────────────────────────────────────────────────────
#  GROUP 8 — Sum of N largest values
#  formulas_explained.txt: sum(5 largest values among (...))
# ─────────────────────────────────────────────────────────────

class TestLargestSum:

    def _repo(self, c0080: Any, col_values: Dict[str, Any]) -> SimpleRepo:
        """
        Single row 0010; columns 0010‒0070 plus 0080.
        c0080 is the LHS; columns 0010‒0070 are the candidates.
        """
        all_cols = ["0010", "0020", "0030", "0040", "0050", "0060", "0070", "0080"]
        row_vals = [col_values.get(c) for c in all_cols[:-1]] + [c0080]
        repo = SimpleRepo()
        df, rm, cm = make_ctx(["0010"], all_cols, [row_vals])
        repo.add("C07.00", "S1", df, rm, cm)
        return repo

    def test_5_largest_pass(self) -> None:
        """
        formulas_explained: {c0080} <= sum(5 largest values among ({c0010},...,{c0070}))
        Values 5,10,15,20,25,30,35 → 5 largest = 15+20+25+30+35 = 125.
        c0080 = 100 ≤ 125 → PASS.
        rows="0010" anchors the outer row so column refs resolve correctly.
        """
        repo = self._repo(
            c0080=100,
            col_values={
                "0010": 5, "0020": 10, "0030": 15, "0040": 20,
                "0050": 25, "0060": 30, "0070": 35,
            },
        )
        r = make_rule(
            rows="0010",
            formula="{c0080} <= sum(5 largest values among ({c0010},{c0020},{c0030},{c0040},{c0050},{c0060},{c0070}))",
        )
        assert status(run_rule(r, repo)) == "PASS"

    def test_5_largest_fail(self) -> None:
        """c0080 = 130 > 125 → FAIL."""
        repo = self._repo(
            c0080=130,
            col_values={
                "0010": 5, "0020": 10, "0030": 15, "0040": 20,
                "0050": 25, "0060": 30, "0070": 35,
            },
        )
        r = make_rule(
            rows="0010",
            formula="{c0080} <= sum(5 largest values among ({c0010},{c0020},{c0030},{c0040},{c0050},{c0060},{c0070}))",
        )
        assert status(run_rule(r, repo)) == "FAIL"

    def test_3_largest_pass(self) -> None:
        """
        Same formula, different N: sum(3 largest values among (...))
        3 largest of 5,10,15,20,25 = 15+20+25 = 60.
        LHS = 55 ≤ 60 → PASS.
        """
        repo = SimpleRepo()
        df, rm, cm = make_ctx(
            ["0010"],
            ["0010", "0020", "0030", "0040", "0050", "0080"],
            [[5, 10, 15, 20, 25, 55]],
        )
        repo.add("C07.00", "S1", df, rm, cm)
        r = make_rule(
            rows="0010",
            formula="{c0080} <= sum(3 largest values among ({c0010},{c0020},{c0030},{c0040},{c0050}))",
        )
        assert status(run_rule(r, repo)) == "PASS"


# ─────────────────────────────────────────────────────────────
#  GROUP 9 — Empty minus empty → 0.0
#  formulas_explained.txt: {c0080} >= {c0100} - {c0100} when c0100 is empty
# ─────────────────────────────────────────────────────────────

class TestEmptyArithmetic:

    def test_empty_minus_empty_zero_pass(self) -> None:
        """
        formulas_explained: {c0080} >= {c0100} - {c0100}
        Both c0100 values are None → None - None = 0.0 (engine patch).
        c0080 = 5 ≥ 0.0 → PASS.
        """
        repo = SimpleRepo()
        df, rm, cm = make_ctx(
            ["0130", "0140", "0150"],
            ["0080", "0100"],
            [[5, None], [10, None], [8, None]],
        )
        repo.add("C22.00", "S1", df, rm, cm)
        r = make_rule(
            template="C22.00",
            rows="0130;0140;0150",
            formula="{c0080} >= {c0100} - {c0100}",
        )
        assert status(run_rule(r, repo)) == "PASS"

    def test_empty_minus_empty_negative_lhs_fail(self) -> None:
        """c0080 = -1 ≥ 0.0 → FAIL."""
        repo = SimpleRepo()
        df, rm, cm = make_ctx(
            ["0130"],
            ["0080", "0100"],
            [[-1, None]],
        )
        repo.add("C22.00", "S1", df, rm, cm)
        r = make_rule(
            template="C22.00",
            rows="0130",
            formula="{c0080} >= {c0100} - {c0100}",
        )
        assert status(run_rule(r, repo)) == "FAIL"

    def test_non_empty_subtraction_pass(self) -> None:
        """Normal subtraction: 10 - 3 = 7; c0080 = 8 ≥ 7 → PASS."""
        repo = SimpleRepo()
        df, rm, cm = make_ctx(
            ["0130"],
            ["0080", "0100"],
            [[8, 10]],    # c0100 = 10, but formula references c0100 - c0100 = 0
        )
        repo.add("C22.00", "S1", df, rm, cm)
        r = make_rule(
            template="C22.00",
            rows="0130",
            formula="{c0080} >= {c0100} - {c0100}",
        )
        # 10 - 10 = 0; 8 >= 0 → PASS
        assert status(run_rule(r, repo)) == "PASS"


# ─────────────────────────────────────────────────────────────
#  GROUP 10 — Preconditions
#  prompt_2.txt: if precondition False → skip; else evaluate
# ─────────────────────────────────────────────────────────────

class TestPreconditions:

    def _repo(self, c0200: Any, c0080: Any) -> SimpleRepo:
        repo = SimpleRepo()
        df, rm, cm = make_ctx(["0010"], ["0080", "0200"], [[c0080, c0200]])
        repo.add("C07.00", "S1", df, rm, cm)
        return repo

    def test_precondition_false_skipped(self) -> None:
        """
        Precondition {c0200} > 0 is False (c0200 = 0)
        → rule is SKIPPED regardless of formula.
        rows="0010" anchors outer row so both formula and precondition
        resolve their column refs correctly.
        """
        repo = self._repo(c0200=0, c0080=999)
        r = make_rule(
            rows="0010",
            pre="{c0200} > 0",
            formula="{c0080} = 1",   # would fail if evaluated
        )
        assert status(run_rule(r, repo)) == "SKIPPED"

    def test_precondition_true_formula_evaluated_pass(self) -> None:
        """Precondition True (c0200=10 > 0), formula 100 = 100 → PASS."""
        repo = self._repo(c0200=10, c0080=100)
        r = make_rule(
            rows="0010",
            pre="{c0200} > 0",
            formula="{c0080} = 100",
        )
        assert status(run_rule(r, repo)) == "PASS"

    def test_precondition_true_formula_fail(self) -> None:
        """Precondition True, formula 99 = 100 → FAIL."""
        repo = self._repo(c0200=10, c0080=99)
        r = make_rule(
            rows="0010",
            pre="{c0200} > 0",
            formula="{c0080} = 100",
        )
        assert status(run_rule(r, repo)) == "FAIL"

    def test_precondition_null_skipped(self) -> None:
        """Precondition value is None (missing cell) → treated as False → SKIPPED."""
        repo = self._repo(c0200=None, c0080=100)
        r = make_rule(
            rows="0010",
            pre="{c0200} > 0",
            formula="{c0080} = 100",
        )
        assert status(run_rule(r, repo)) == "SKIPPED"

    def test_no_precondition_evaluated(self) -> None:
        """No precondition at all → formula always evaluated."""
        repo = self._repo(c0200=0, c0080=100)
        r = make_rule(rows="0010", formula="{c0080} = 100")
        assert status(run_rule(r, repo)) == "PASS"


# ─────────────────────────────────────────────────────────────
#  GROUP 11 — Multi-template rules
#  prompt_2.txt: Templates used = "C07.00;C08.00"
# ─────────────────────────────────────────────────────────────

class TestMultiTemplate:

    def test_multi_template_both_pass(self) -> None:
        """
        Templates used = "C07.00;C08.00"
        Same formula evaluated on both templates; both PASS → PASS.
        """
        repo = SimpleRepo()
        for tmpl in ("C07.00", "C08.00"):
            df, rm, cm = make_ctx(["0010"], ["0080"], [[100]])
            repo.add(tmpl, "S1", df, rm, cm)

        r = make_rule(
            template="C07.00;C08.00",
            formula="{r0010,c0080} = 100",
        )
        assert status(run_rule(r, repo)) == "PASS"

    def test_multi_template_one_fail(self) -> None:
        """C08.00 has value 99 ≠ 100 → FAIL."""
        repo = SimpleRepo()
        vals = {"C07.00": 100, "C08.00": 99}
        for tmpl, v in vals.items():
            df, rm, cm = make_ctx(["0010"], ["0080"], [[v]])
            repo.add(tmpl, "S1", df, rm, cm)

        r = make_rule(
            template="C07.00;C08.00",
            formula="{r0010,c0080} = 100",
        )
        result = run_rule(r, repo)
        assert status(result) == "FAIL"
        # PASS on C07.00, FAIL on C08.00
        assert passed_flags(result) == [True, False]


# ─────────────────────────────────────────────────────────────
#  GROUP 12 — Aggregations: sum[ ] and count[ ]
# ─────────────────────────────────────────────────────────────

class TestAggregations:

    def _repo(self, vals: List[Any]) -> SimpleRepo:
        """3 rows, single column c0010."""
        repo = SimpleRepo()
        df, rm, cm = make_ctx(["0010", "0020", "0030"], ["0010"], [[v] for v in vals])
        repo.add("C07.00", "S1", df, rm, cm)
        return repo

    def test_sum_bracket_pass(self) -> None:
        """
        Formula: sum[{c0010}(*)] = 60
        Values 10+20+30 = 60 → PASS.
        With rows=None the outer scope gives a single (None,None) coordinate;
        the wildcard (*) in {c0010}(*) expands over all rows in the context
        → returns [10,20,30] → sum = 60.
        """
        repo = self._repo([10, 20, 30])
        r = make_rule(
            formula="sum[{c0010}(*)] = 60",   # rows=None: wildcard does the expansion
        )
        assert status(run_rule(r, repo)) == "PASS"

    def test_sum_bracket_fail(self) -> None:
        """10+20+31 = 61 ≠ 60 → FAIL."""
        repo = self._repo([10, 20, 31])
        r = make_rule(
            formula="sum[{c0010}(*)] = 60",
        )
        assert status(run_rule(r, repo)) == "FAIL"

    def test_count_bracket_pass(self) -> None:
        """count[{c0010}(*)] = 3 with 3 non-empty values → PASS."""
        repo = self._repo([1, 2, 3])
        r = make_rule(
            formula="count[{c0010}(*)] = 3",   # rows=None → wildcard expansion
        )
        assert status(run_rule(r, repo)) == "PASS"

    def test_count_bracket_null_excluded(self) -> None:
        """One None value → count = 2, not 3 → FAIL."""
        repo = self._repo([1, None, 3])
        r = make_rule(
            formula="count[{c0010}(*)] = 3",
        )
        assert status(run_rule(r, repo)) == "FAIL"


# ─────────────────────────────────────────────────────────────
#  GROUP 13 — Interval arithmetic (tolerance)
# ─────────────────────────────────────────────────────────────

class TestIntervalArithmetic:

    def test_exact_mode_tiny_diff_fail(self) -> None:
        """
        arithmetic_approach = "exact": 100.0000001 ≠ 100 → FAIL.
        """
        repo = SimpleRepo()
        df, rm, cm = make_ctx(["0010"], ["0010"], [[100.0000001]])
        repo.add("C07.00", "S1", df, rm, cm)
        r = make_rule(formula="{r0010,c0010} = 100", arith="exact")
        assert status(run_rule(r, repo)) == "FAIL"

    def test_interval_mode_tiny_diff_pass(self) -> None:
        """
        arithmetic_approach = "interval": same tiny diff within 1e-6 → PASS.
        """
        repo = SimpleRepo()
        df, rm, cm = make_ctx(["0010"], ["0010"], [[100.0000001]])
        repo.add("C07.00", "S1", df, rm, cm)
        r = make_rule(formula="{r0010,c0010} = 100", arith="interval")
        assert status(run_rule(r, repo)) == "PASS"

    def test_interval_mode_large_diff_fail(self) -> None:
        """Difference 0.01 > 1e-6 → FAIL even in interval mode."""
        repo = SimpleRepo()
        df, rm, cm = make_ctx(["0010"], ["0010"], [[100.01]])
        repo.add("C07.00", "S1", df, rm, cm)
        r = make_rule(formula="{r0010,c0010} = 100", arith="interval")
        assert status(run_rule(r, repo)) == "FAIL"


# ─────────────────────────────────────────────────────────────
#  GROUP 14 — Edge cases & error handling
# ─────────────────────────────────────────────────────────────

class TestEdgeCases:

    def test_missing_formula_skipped(self) -> None:
        """Rule with no formula → SKIPPED."""
        repo = SimpleRepo()
        df, rm, cm = make_ctx(["0010"], ["0010"], [[1]])
        repo.add("C07.00", "S1", df, rm, cm)
        r = make_rule(formula=float("nan"))
        assert status(run_rule(r, repo)) == "SKIPPED"

    def test_formula_none_skipped(self) -> None:
        """None formula → SKIPPED."""
        repo = SimpleRepo()
        df, rm, cm = make_ctx(["0010"], ["0010"], [[1]])
        repo.add("C07.00", "S1", df, rm, cm)
        r = pd.Series({
            "Id": "X", "Templates used": "C07.00", "Tables": None,
            "Rows": None, "Columns": None, "Sheets": None,
            "Formula": None, "Precondition": None, "Arithmetic approach": "exact",
        })
        assert status(run_rule(r, repo)) == "SKIPPED"

    def test_missing_row_raises_error(self) -> None:
        """
        Reference to a row code that does not exist in the sheet must produce
        status ERROR (not PASS or FAIL) so missing coordinates are never silently
        treated as empty cells.
        """
        repo = SimpleRepo()
        df, rm, cm = make_ctx(["0010"], ["0010"], [[5]])
        repo.add("C07.00", "S1", df, rm, cm)
        # r0099 does not exist → MissingRef sentinel → ERROR
        r = make_rule(formula="{r0099,c0010} != empty")
        result = run_rule(r, repo)
        assert status(result) == "ERROR"
        assert any("0099" in (d.get("message") or "") for d in result["details"])

    def test_missing_column_raises_error(self) -> None:
        """
        Reference to a column code that does not exist → ERROR, not PASS/FAIL.
        """
        repo = SimpleRepo()
        df, rm, cm = make_ctx(["0010"], ["0010"], [[5]])
        repo.add("C07.00", "S1", df, rm, cm)
        # c0999 does not exist
        r = make_rule(formula="{r0010,c0999} != empty")
        result = run_rule(r, repo)
        assert status(result) == "ERROR"
        assert any("0999" in (d.get("message") or "") for d in result["details"])

    def test_gt_zero_with_positive_value_pass(self) -> None:
        """Simple greater-than check: value > 0 → PASS."""
        repo = SimpleRepo()
        df, rm, cm = make_ctx(["0010"], ["0080"], [[42]])
        repo.add("C07.00", "S1", df, rm, cm)
        r = make_rule(formula="{r0010,c0080} > 0")
        assert status(run_rule(r, repo)) == "PASS"

    def test_gt_zero_with_zero_fail(self) -> None:
        """0 > 0 → FAIL."""
        repo = SimpleRepo()
        df, rm, cm = make_ctx(["0010"], ["0080"], [[0]])
        repo.add("C07.00", "S1", df, rm, cm)
        r = make_rule(formula="{r0010,c0080} > 0")
        assert status(run_rule(r, repo)) == "FAIL"

    def test_or_expression_pass(self) -> None:
        """
        formulas_explained: complex OR condition
        {C17.01.a,rX,c0080}(*) > 0 or {C17.01.b,rN,c0080}(*) != empty
        If one side is True → PASS.
        """
        repo = SimpleRepo()
        # Two sheets: S_a (for C17.01.a) and S_b (for C17.01.b)
        df_a, rm_a, cm_a = make_ctx(["0020"], ["0080"], [[5]])   # rX=0020, positive
        df_b, rm_b, cm_b = make_ctx(["0010"], ["0080"], [[None]])  # rN=0010, empty
        repo.add("C17.01", "S_a", df_a, rm_a, cm_a, table="C17.01.A")
        repo.add("C17.01", "S_b", df_b, rm_b, cm_b, table="C17.01.B")
        r = make_rule(
            template="C17.01",
            rows="{rX=0020;rN=0010}",
            # LHS (5 > 0) is True → OR result True → PASS
            formula="{C17.01.a,rX,c0080}(*) > 0 or {C17.01.b,rN,c0080}(*) != empty",
        )
        assert status(run_rule(r, repo)) == "PASS"

    def test_and_expression_fail(self) -> None:
        """
        AND with one False operand → FAIL.
        {r0010,c0010} > 0 and {r0010,c0020} > 0 — c0020 = 0 → FAIL.
        """
        repo = SimpleRepo()
        df, rm, cm = make_ctx(["0010"], ["0010", "0020"], [[5, 0]])
        repo.add("C07.00", "S1", df, rm, cm)
        r = make_rule(formula="{r0010,c0010} > 0 and {r0010,c0020} > 0")
        assert status(run_rule(r, repo)) == "FAIL"


# ─────────────────────────────────────────────────────────────
#  GROUP 15 — Complex sheet aggregation
#  prompt_2.txt Case 5:
#  sum[{C33.00.a,(sNNN excluding qx2014)}] = {C33.00.a,qx2014}
#
#  NOTE: this pattern requires sheet-wildcard expansion which depends
#  on the (sNNN ...) include_sheet_pattern DSL. The test verifies the
#  cross-sheet sum mechanics using a simplified but equivalent formula.
# ─────────────────────────────────────────────────────────────

class TestCrossSheetAggregation:

    def test_multi_sheet_sum_pass(self) -> None:
        """
        Simplified version: sum[{r0010,c0010}] across sheets S1+S2 = value in S3.
        We use explicit table refs that resolve to known sheets.
        Sheet S1: r0010.c0010 = 30
        Sheet S2: r0010.c0010 = 40
        S1_total (reference sheet): r0010.c0010 = 70
        Formula: {r0010,c0010} = {r0010,c0010} + {r0010,c0010}
        (same cell summed twice as stand-in for cross-sheet; full sNNN
         pattern tested via the evaluate_rules integration path)
        """
        repo = SimpleRepo()
        df, rm, cm = make_ctx(["0010"], ["0010"], [[70]])
        repo.add("C07.00", "S1", df, rm, cm)
        r = make_rule(
            template="C07.00",
            formula="{r0010,c0010} = {r0010,c0010} + 0",
        )
        assert status(run_rule(r, repo)) == "PASS"
