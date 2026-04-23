from __future__ import annotations

from contextlib import contextmanager
import warnings
from typing import Any, Iterator

import openpyxl
import pandas as pd


_OPENPYXL_DEFAULT_STYLE_WARNING = (
    "Workbook contains no default style, apply openpyxl's default"
)


@contextmanager
def suppress_openpyxl_style_warning() -> Iterator[None]:
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=_OPENPYXL_DEFAULT_STYLE_WARNING,
            category=UserWarning,
        )
        yield


def read_excel_quiet(*args: Any, **kwargs: Any) -> pd.DataFrame:
    with suppress_openpyxl_style_warning():
        return pd.read_excel(*args, **kwargs)


def load_workbook_quiet(*args: Any, **kwargs: Any) -> openpyxl.Workbook:
    with suppress_openpyxl_style_warning():
        return openpyxl.load_workbook(*args, **kwargs)