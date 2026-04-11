# src/io_utils.py
from __future__ import annotations
import csv
from datetime import datetime
from pathlib import Path

import pandas as pd


TRANSACTION_COLUMNS = [
    "Date", "Payee", "Amount", "Source", "Filepath",
    "Year", "Month", "Month_Val", "Flow", "Category",
]

PORTFOLIO_COLUMNS = [
    "Account_Symbol", "Quantity", "Average_Cost_Basis", "Filepath",
]


def ensure_dirs(base: Path) -> dict[str, Path]:
    """Create required directories and return a path map."""
    paths = {
        "apple":     base / "Statements" / "Apple",      # legacy — kept for compat
        "fidelity":  base / "Statements" / "Fidelity",   # legacy
        "credit":    base / "Statements" / "Credit",
        "debit":     base / "Statements" / "Debit",
        "portfolio": base / "Statements" / "Portfolio",   # legacy
        "investments": base / "Statements" / "Investments",
        "output":    base / "output",
        "statements_root": base / "Statements",
    }
    for p in paths.values():
        p.mkdir(parents=True, exist_ok=True)
    return paths


def normalize_and_enrich(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise Date column to YYYY-MM-DD string and add derived columns:
      Year, Month (int), Month_Val (zero-padded str), Flow (IN/OUT)
    """
    if df.empty:
        for col in ["Year", "Month", "Month_Val", "Flow", "Category"]:
            if col not in df.columns:
                df[col] = None
        return df

    df = df.copy()

    # Parse dates robustly
    df["Date"] = pd.to_datetime(df["Date"], infer_datetime_format=True, errors="coerce")
    df = df.dropna(subset=["Date"])
    df["Date"]      = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"]      = pd.to_datetime(df["Date"]).dt.year
    df["Month"]     = pd.to_datetime(df["Date"]).dt.month
    df["Month_Val"] = df["Month"].apply(lambda m: f"{m:02d}")

    # Flow direction
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce").fillna(0)
    df["Flow"]   = df["Amount"].apply(lambda v: "IN" if v < 0 else "OUT")

    # Default category
    if "Category" not in df.columns:
        df["Category"] = "All-Other"

    return df


def write_transactions_csv(df: pd.DataFrame, output_dir: Path) -> Path:
    """Write transactions DataFrame to output/transactions.csv."""
    output_dir.mkdir(parents=True, exist_ok=True)
    out = output_dir / "transactions.csv"

    cols = [c for c in TRANSACTION_COLUMNS if c in df.columns]
    df[cols].to_csv(out, index=False, encoding="utf-8")
    return out


def write_portfolio_csv(df: pd.DataFrame, output_dir: Path) -> Path:
    """Write portfolio positions DataFrame to output/portfolio_positions.csv."""
    output_dir.mkdir(parents=True, exist_ok=True)
    out = output_dir / "portfolio_positions.csv"

    cols = [c for c in PORTFOLIO_COLUMNS if c in df.columns]
    df[cols].to_csv(out, index=False, encoding="utf-8")
    return out
