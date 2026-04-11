"""
investments_parser.py
=====================
Parses Fidelity Portfolio Positions CSV exports into a standardized DataFrame.

Expected filename pattern: Portfolio_Positions_MMM-DD-YYYY.csv
Expected columns (Fidelity default export):
    Account Number, Account Name, Symbol, Description, Quantity,
    Last Price, Last Price Change, Current Value, Today's Gain/Loss Dollar,
    Today's Gain/Loss Percent, Total Gain/Loss Dollar, Total Gain/Loss Percent,
    Percent Of Account, Cost Basis Total, Average Cost Basis, Type

Output DataFrame columns:
    Account_Symbol, Quantity, Average_Cost_Basis, Filepath
"""

from __future__ import annotations
import logging
import re
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

# Fidelity uses these sentinel rows to separate account blocks
_SKIP_SYMBOLS = {"", "pending", "cash", "n/a"}
_SKIP_DESC    = {"account total", "brokerage services"}


def _extract_snapshot_date(path: Path) -> str | None:
    """Pull MMM-DD-YYYY from filename e.g. Portfolio_Positions_Apr-07-2026.csv"""
    m = re.search(r"(\w{3}-\d{2}-\d{4})", path.stem)
    return m.group(1) if m else None


def _clean_numeric(val) -> float:
    """Strip $ % commas and cast to float. Returns 0.0 on failure."""
    try:
        return float(re.sub(r"[$%,\s]", "", str(val)))
    except (ValueError, TypeError):
        return 0.0


def parse_investments_csv(path: Path) -> pd.DataFrame:
    """
    Parse a single Fidelity Portfolio Positions CSV.
    Returns DataFrame with columns: Account_Symbol, Quantity, Average_Cost_Basis, Filepath
    """
    try:
        # Fidelity prepends account header rows before the column header —
        # find the real header row by looking for 'Symbol'
        raw = path.read_text(encoding="utf-8-sig", errors="replace").splitlines()
        header_idx = 0
        for i, line in enumerate(raw[:20]):
            if "Symbol" in line and "Quantity" in line:
                header_idx = i
                break

        df = pd.read_csv(path, skiprows=header_idx, encoding="utf-8-sig",
                         on_bad_lines="skip", dtype=str)
        df.columns = df.columns.str.strip()

    except Exception as e:
        log.error(f"[InvestmentsParser] Failed to read {path.name}: {e}")
        return pd.DataFrame()

    required_candidates = [
        {"Symbol", "Quantity", "Average Cost Basis"},
        {"Symbol", "Quantity", "Average Cost Basis Per Share"},
        {"Symbol", "Quantity", "Cost Basis Total"},
    ]
    matched_cols = None
    for candidate in required_candidates:
        if candidate.issubset(set(df.columns)):
            matched_cols = candidate
            break

    if not matched_cols:
        # Try case-insensitive match
        col_lower = {c.lower(): c for c in df.columns}
        for alias in ["average cost basis per share", "average cost basis",
                      "cost basis total", "avg cost basis", "avg. cost basis"]:
            if alias in col_lower:
                matched_cols = {"Symbol", "Quantity", col_lower[alias]}
                break

    if not matched_cols:
        log.warning(f"[InvestmentsParser] {path.name}: missing cost basis column. "
                    f"Columns found: {list(df.columns)}")
        # Proceed without cost basis rather than failing completely
        acb_col = None
    else:
        acb_col = next((c for c in matched_cols
                        if "cost" in c.lower() or "basis" in c.lower()), None)

    # Detect account number column (varies slightly by export version)
    acct_col = next(
        (c for c in df.columns if "account" in c.lower() and "number" in c.lower()),
        next((c for c in df.columns if "account" in c.lower()), None)
    )

    rows = []
    current_account = "Unknown"

    for _, row in df.iterrows():
        sym  = str(row.get("Symbol", "")).strip()
        desc = str(row.get("Description", "")).strip().lower()

        # Track current account from header rows Fidelity injects
        if acct_col and str(row.get(acct_col, "")).strip():
            acct_val = str(row[acct_col]).strip()
            if acct_val and acct_val.lower() not in ("nan", "account number"):
                current_account = acct_val

        # Skip sentinel / summary rows
        if sym.lower() in _SKIP_SYMBOLS:
            continue
        if any(skip in desc for skip in _SKIP_DESC):
            continue
        if sym.startswith("--") or not sym:
            continue

        qty  = _clean_numeric(row.get("Quantity", 0))
        # Cost basis: "--" means not tracked (tax-advantaged accounts) — store as None
        acb_raw = row.get(acb_col, "--") if acb_col else "--"
        acb_str = str(acb_raw).strip()
        if acb_str in ("--", "n/a", "na", "", "nan"):
            acb = None   # not available — will show as N/A in UI
        else:
            acb = _clean_numeric(acb_raw)
            if acb == 0.0 and acb_str not in ("0", "0.0", "$0.00"):
                acb = None  # 0 from failed parse, treat as unknown

        if qty == 0:
            continue

        rows.append({
            "Account_Symbol":     f"{current_account}_{sym}",
            "Quantity":           qty,
            "Average_Cost_Basis": acb,  # None = not available (IRA/Roth)
            "Filepath":           str(path),
        })

    if not rows:
        log.warning(f"[InvestmentsParser] {path.name}: no valid positions found")
        return pd.DataFrame()

    result = pd.DataFrame(rows)
    log.info(f"[InvestmentsParser] {path.name}: {len(result)} positions")
    return result


def parse_investments_csv_dir(directory: Path) -> pd.DataFrame:
    """
    Parse all Portfolio_Positions_*.csv files in a directory.
    Uses the most recent file by snapshot date if multiple exist.
    Returns combined DataFrame (may be empty).
    """
    if not directory.exists():
        log.warning(f"[InvestmentsParser] Directory not found: {directory}")
        return pd.DataFrame()

    files = sorted(directory.glob("Portfolio_Positions_*.csv"))
    if not files:
        # Fallback: accept any CSV in the folder
        files = sorted(directory.glob("*.csv"))

    if not files:
        log.info(f"[InvestmentsParser] No CSVs found in {directory}")
        return pd.DataFrame()

    # Use the last file (most recent by filename sort — Fidelity dates sort correctly)
    path = files[-1]
    log.info(f"[InvestmentsParser] Parsing {path.name}")
    return parse_investments_csv(path)
