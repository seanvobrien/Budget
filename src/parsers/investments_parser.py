"""
investments_parser.py
=====================
Parses Fidelity Portfolio Positions CSV exports.

Key fix: uses "Cost Basis Total" / Quantity to compute average cost,
because "Average Cost Basis" is "--" for tax-advantaged accounts (IRA/Roth).

Expected filename: Portfolio_Positions_MMM-DD-YYYY.csv
"""
from __future__ import annotations
import logging
import re
from pathlib import Path
from datetime import datetime

import pandas as pd

log = logging.getLogger(__name__)


def _clean_headers(cols) -> list[str]:
    return (
        pd.Series(cols)
        .astype(str)
        .str.replace("\u00A0", " ", regex=False)   # non-breaking space
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", " ", regex=True)
        .str.replace(r"[\"']", "", regex=True)
        .tolist()
    )


def _clean_numeric(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip()
    s = s.str.replace(r"^\((.*)\)$", r"-\1", regex=True)   # (123.45) → -123.45
    s = s.str.replace(r"[\$,]", "", regex=True)
    s = s.replace({"": None, "--": None, "n/a": None, "nan": None})
    return pd.to_numeric(s, errors="coerce")


def parse_investments_csv(path: Path) -> pd.DataFrame:
    log.info(f"[Portfolio] Parsing {path.name}")

    df = pd.read_csv(
        path,
        encoding="utf-8-sig",
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        index_col=False,
    )
    df.columns = _clean_headers(df.columns)

    required = {"account number", "symbol", "quantity", "cost basis total"}
    missing = required - set(df.columns)
    if missing:
        log.warning(f"[Portfolio] {path.name}: missing columns {missing}. Found: {df.columns.tolist()}")
        return pd.DataFrame()

    # Strip whitespace everywhere
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()

    # Remove non-position rows: empty symbol, ** summary rows, pending
    df = df[df["symbol"] != ""]
    df = df[~df["symbol"].str.contains(r"\*\*", regex=True, na=False)]
    df = df[~df["symbol"].str.contains("pending", case=False, na=False)]

    # Numeric cleanup
    df["quantity"]          = _clean_numeric(df["quantity"])
    df["cost basis total"]  = _clean_numeric(df["cost basis total"])

    df = df.dropna(subset=["quantity", "cost basis total"])
    df = df[df["quantity"] > 0]

    if df.empty:
        log.warning(f"[Portfolio] {path.name}: no valid positions after filtering")
        return pd.DataFrame()

    # Compute per-share average cost from total
    df["average_cost_basis"] = df["cost basis total"] / df["quantity"]

    result = pd.DataFrame({
        "Account_Symbol":    df["account number"] + "_" + df["symbol"],
        "Quantity":          df["quantity"],
        "Average_Cost_Basis": df["average_cost_basis"],
        "Filepath":          str(path),
    })

    log.info(f"[Portfolio] {path.name}: {len(result)} positions")
    return result


def parse_investments_csv_dir(directory: Path) -> pd.DataFrame:
    if not directory.exists():
        log.warning(f"[Portfolio] Directory not found: {directory}")
        return pd.DataFrame()

    # Select most recent file by date in filename
    pattern = re.compile(r"portfolio_positions_(\w{3}-\d{2}-\d{4})\.csv", re.IGNORECASE)
    dated: list[tuple[datetime, Path]] = []

    for p in directory.glob("*.csv"):
        m = pattern.match(p.name)
        if m:
            try:
                dt = datetime.strptime(m.group(1), "%b-%d-%Y")
                dated.append((dt, p))
            except ValueError:
                pass

    if not dated:
        # Fallback: any CSV in folder
        files = sorted(directory.glob("*.csv"))
        if not files:
            log.info(f"[Portfolio] No CSVs in {directory}")
            return pd.DataFrame()
        return parse_investments_csv(files[-1])

    dated.sort(key=lambda x: x[0], reverse=True)
    return parse_investments_csv(dated[0][1])
