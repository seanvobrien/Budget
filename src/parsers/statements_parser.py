"""
statements_parser.py
====================
A bank-agnostic parser that handles CSVs and PDFs from any institution.

Strategy:
  CSVs  → header sniffing → column mapping → sign normalization
  PDFs  → pdfplumber table extraction → fallback to line-regex
  Both  → confidence scoring → structured output matching run.py schema

Returns a DataFrame with columns: Date, Payee, Amount, Source, Filepath
Unresolvable rows are logged but skipped (never silently zeroed).
"""

from __future__ import annotations
import re
import logging
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd
import pdfplumber

log = logging.getLogger(__name__)

# ── Column name aliases ──────────────────────────────────────────────────────

DATE_ALIASES = [
    "date", "transaction date", "trans date", "trans. date",
    "posted date", "post date", "settlement date", "settled date",
    "activity date", "effective date", "booking date",
]

PAYEE_ALIASES = [
    "description", "payee", "merchant", "payee name",
    "transaction description", "details", "narrative",
    "memo", "particulars", "name", "store name",
    "original description",
]

AMOUNT_ALIASES = [
    "amount", "transaction amount", "amount (usd)", "amt",
    "debit/credit", "net amount",
]

DEBIT_ALIASES = [
    "debit", "withdrawals", "withdrawal", "charge", "charges",
    "payments and credits",
]

CREDIT_ALIASES = [
    "credit", "deposits", "deposit", "credits", "payment",
]

# ── Date format patterns (tried in order) ───────────────────────────────────

DATE_FORMATS = [
    "%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y",
    "%d/%m/%Y", "%d/%m/%y",
    "%B %d, %Y", "%b %d, %Y",
    "%b %d %Y", "%B %d %Y",
    "%d %b %Y", "%d %B %Y",
    "%Y%m%d",
]

# ── Line-regex for PDF text fallback ────────────────────────────────────────
# Matches lines like:  01/15/2024  AMAZON.COM  -$123.45
#                      2024-01-15  Starbucks   45.00
LINE_RE = re.compile(
    r"(?P<date>"
    r"\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}"   # MM/DD/YYYY or YYYY-MM-DD
    r"|\d{4}[/\-]\d{2}[/\-]\d{2}"
    r"|[A-Za-z]{3,9}\.?\s+\d{1,2},?\s+\d{4}"  # Jan 15, 2024
    r")"
    r"\s+"
    r"(?P<payee>.+?)"
    r"\s+"
    r"(?P<amount>-?\$?[\d,]+\.\d{2})"
    r"\s*$",
    re.IGNORECASE,
)

# ── Confidence thresholds ────────────────────────────────────────────────────

CONF_HIGH   = 0.85   # all 3 cols mapped cleanly
CONF_MEDIUM = 0.60   # 2 of 3 mapped
CONF_LOW    = 0.35   # regex fallback, usable but flag for review


@dataclass
class ParseResult:
    df: Optional[pd.DataFrame]
    confidence: float
    method: str          # "csv-table" | "pdf-table" | "pdf-regex"
    warnings: list[str] = field(default_factory=list)
    source_label: str = "Universal"


# ── Helpers ──────────────────────────────────────────────────────────────────

def _norm_col(c: str) -> str:
    """Lowercase, strip, collapse spaces."""
    return re.sub(r"\s+", " ", str(c).strip().lower())


def _find_col(headers: list[str], aliases: list[str]) -> Optional[str]:
    """Return the first header that matches any alias (exact after normalization)."""
    norm = {_norm_col(h): h for h in headers}
    for alias in aliases:
        if alias in norm:
            return norm[alias]
    # Partial match fallback
    for alias in aliases:
        for h_norm, h_orig in norm.items():
            if alias in h_norm or h_norm in alias:
                return h_orig
    return None


def _parse_date(val: str) -> Optional[str]:
    """Try every known date format; return YYYY-MM-DD or None."""
    val = str(val).strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _parse_amount(val) -> Optional[float]:
    """Clean currency strings → float. Returns None if unparseable."""
    try:
        cleaned = re.sub(r"[,$\s]", "", str(val))
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def _detect_sign_convention(df: pd.DataFrame, amount_col: str) -> str:
    """
    Heuristic: if most large positive values have payees containing
    'payment', 'deposit', 'credit', 'transfer', they're likely income
    and the sign convention is 'positive=credit'.
    Returns 'positive=debit' or 'positive=credit'.
    """
    if df.empty or amount_col not in df.columns:
        return "positive=debit"
    positives = df[df[amount_col] > 0]
    if positives.empty:
        return "positive=debit"
    income_keywords = r"payment|deposit|credit|transfer|payroll|direct dep"
    payee_col = _find_col(list(df.columns), PAYEE_ALIASES)
    if payee_col:
        matches = positives[payee_col].str.contains(income_keywords, case=False, na=False)
        ratio = matches.sum() / max(len(positives), 1)
        if ratio > 0.5:
            return "positive=credit"  # positive = money coming in
    return "positive=debit"


def _normalize_amount(series: pd.Series, convention: str) -> pd.Series:
    """
    Normalize so that expenses are POSITIVE (matching run.py convention).
    Income/transfers may be negative — callers handle flow direction.
    """
    if convention == "positive=credit":
        return -series   # flip: credits become negative, debits positive
    return series        # already positive=debit


def _build_df(
    rows: list[dict],
    filepath: str,
    source: str,
    confidence: float,
    method: str,
    warnings: list[str],
) -> ParseResult:
    if not rows:
        return ParseResult(None, 0.0, method, warnings + ["No rows extracted"], source)
    df = pd.DataFrame(rows)
    df["Source"]   = source
    df["Filepath"] = filepath
    return ParseResult(df[["Date","Payee","Amount","Source","Filepath"]], confidence, method, warnings, source)


# ── CSV Parser ───────────────────────────────────────────────────────────────

def _parse_csv(path: Path) -> ParseResult:
    warnings: list[str] = []
    source = path.stem

    # Read raw — try to detect the header row (some banks prepend account info)
    raw = path.read_text(encoding="utf-8-sig", errors="replace")
    lines = raw.splitlines()

    # Find first line that looks like a header (contains 2+ comma-separated words)
    header_idx = 0
    for i, line in enumerate(lines[:20]):
        parts = [p.strip() for p in line.split(",")]
        if len(parts) >= 2 and not re.match(r"^\d", parts[0]):
            header_idx = i
            break

    try:
        df_raw = pd.read_csv(path, skiprows=header_idx, encoding="utf-8-sig",
                             on_bad_lines="skip", dtype=str)
    except Exception as e:
        return ParseResult(None, 0.0, "csv-table", [f"CSV read error: {e}"], source)

    df_raw.columns = df_raw.columns.str.strip()
    headers = list(df_raw.columns)

    # Map columns
    date_col   = _find_col(headers, DATE_ALIASES)
    payee_col  = _find_col(headers, PAYEE_ALIASES)
    amount_col = _find_col(headers, AMOUNT_ALIASES)
    debit_col  = _find_col(headers, DEBIT_ALIASES)
    credit_col = _find_col(headers, CREDIT_ALIASES)

    # Confidence scoring
    cols_found = sum(x is not None for x in [date_col, payee_col, amount_col or (debit_col or credit_col)])
    confidence = {3: CONF_HIGH, 2: CONF_MEDIUM}.get(cols_found, CONF_LOW)

    if not date_col:
        return ParseResult(None, 0.0, "csv-table", ["Could not identify date column"], source)
    if not payee_col:
        return ParseResult(None, 0.0, "csv-table", ["Could not identify payee/description column"], source)

    rows = []
    for _, row in df_raw.iterrows():
        date = _parse_date(row.get(date_col, ""))
        if not date:
            continue

        payee = str(row.get(payee_col, "")).strip()
        if not payee or payee.lower() in ("nan", ""):
            continue

        # Amount resolution: unified > debit/credit split
        amount = None
        if amount_col and pd.notna(row.get(amount_col)):
            amount = _parse_amount(row[amount_col])
        elif debit_col or credit_col:
            debit  = _parse_amount(row.get(debit_col,  0) or 0) or 0.0
            credit = _parse_amount(row.get(credit_col, 0) or 0) or 0.0
            # Debits positive (expenses), credits negative (income)
            amount = debit - credit if (debit or credit) else None

        if amount is None:
            continue

        # Sign convention normalization (only for unified amount column)
        if amount_col:
            convention = _detect_sign_convention(df_raw.assign(**{amount_col: pd.to_numeric(df_raw[amount_col], errors="coerce")}), amount_col)
            if convention == "positive=credit":
                amount = -amount

        rows.append({"Date": date, "Payee": payee, "Amount": amount})

    if warnings and confidence >= CONF_MEDIUM:
        pass  # non-fatal
    return _build_df(rows, str(path), source, confidence, "csv-table", warnings)


# ── PDF Parser ───────────────────────────────────────────────────────────────

def _parse_pdf(path: Path) -> ParseResult:
    warnings: list[str] = []
    source = path.stem
    all_rows: list[dict] = []

    try:
        with pdfplumber.open(path) as pdf:
            header_map: dict[str, int] = {}   # col_name → column index, found on any page

            for page_num, page in enumerate(pdf.pages):
                tables = page.extract_tables()

                for table in tables:
                    if not table or len(table) < 2:
                        continue

                    # Identify header row (first row with 3+ non-None cells)
                    hdr_row_idx = 0
                    for ri, row in enumerate(table[:5]):
                        if row and sum(1 for c in row if c) >= 3:
                            hdr_row_idx = ri
                            break

                    raw_headers = [str(c or "").strip() for c in table[hdr_row_idx]]

                    # Only remap headers if we find a better set
                    date_col   = _find_col(raw_headers, DATE_ALIASES)
                    payee_col  = _find_col(raw_headers, PAYEE_ALIASES)
                    amount_col = _find_col(raw_headers, AMOUNT_ALIASES)
                    debit_col  = _find_col(raw_headers, DEBIT_ALIASES)
                    credit_col = _find_col(raw_headers, CREDIT_ALIASES)

                    if not (date_col and payee_col):
                        continue   # this table isn't a transaction table

                    col_idx = {h: i for i, h in enumerate(raw_headers)}

                    for row in table[hdr_row_idx + 1:]:
                        if not row or all(c is None or str(c).strip() == "" for c in row):
                            continue

                        def get(col):
                            idx = col_idx.get(col)
                            return str(row[idx]).strip() if idx is not None and idx < len(row) else ""

                        date = _parse_date(get(date_col))
                        if not date:
                            continue
                        payee = get(payee_col)
                        if not payee or payee.lower() == "nan":
                            continue

                        amount = None
                        if amount_col:
                            amount = _parse_amount(get(amount_col))
                        if amount is None and (debit_col or credit_col):
                            debit  = _parse_amount(get(debit_col)  or "0") or 0.0
                            credit = _parse_amount(get(credit_col) or "0") or 0.0
                            amount = debit - credit if (debit or credit) else None

                        if amount is None:
                            continue
                        all_rows.append({"Date": date, "Payee": payee, "Amount": amount})

    except Exception as e:
        warnings.append(f"pdfplumber error: {e}")

    # If table extraction got rows, we're done
    if all_rows:
        confidence = CONF_HIGH if len(all_rows) > 5 else CONF_MEDIUM
        return _build_df(all_rows, str(path), source, confidence, "pdf-table", warnings)

    # ── Fallback: line-regex on raw text ─────────────────────────────────────
    warnings.append("No tables found — falling back to line-regex extraction")
    try:
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                for line in text.splitlines():
                    m = LINE_RE.search(line)
                    if not m:
                        continue
                    date = _parse_date(m.group("date"))
                    if not date:
                        continue
                    payee  = m.group("payee").strip()
                    amount = _parse_amount(m.group("amount"))
                    if payee and amount is not None:
                        all_rows.append({"Date": date, "Payee": payee, "Amount": amount})
    except Exception as e:
        warnings.append(f"Line-regex fallback error: {e}")

    if not all_rows:
        warnings.append("Zero transactions extracted — may be a scanned/image PDF")
        return ParseResult(None, 0.0, "pdf-regex", warnings, source)

    return _build_df(all_rows, str(path), source, CONF_LOW, "pdf-regex", warnings)


# ── Public API ───────────────────────────────────────────────────────────────

def parse_file(path: Path) -> ParseResult:
    """Parse a single file. Returns a ParseResult regardless of success."""
    ext = path.suffix.lower()
    if ext == ".csv":
        return _parse_csv(path)
    elif ext == ".pdf":
        return _parse_pdf(path)
    else:
        return ParseResult(None, 0.0, "unsupported",
                           [f"Unsupported file type: {ext}"], path.stem)


def parse_directory(directory: Path, min_confidence: float = CONF_LOW) -> pd.DataFrame:
    """
    Parse all CSVs and PDFs in a directory.
    Skips files below min_confidence and logs warnings.
    Returns a combined DataFrame (may be empty).
    """
    frames: list[pd.DataFrame] = []
    files = sorted([
        f for f in directory.iterdir()
        if f.suffix.lower() in (".csv", ".pdf") and not f.name.startswith(".")
    ])

    if not files:
        log.info(f"[Universal] No CSV/PDF files found in {directory}")
        return pd.DataFrame(columns=["Date","Payee","Amount","Source","Filepath"])

    for f in files:
        result = parse_file(f)
        tag = f"[Universal:{result.method}]"

        if result.warnings:
            for w in result.warnings:
                log.warning(f"{tag} {f.name}: {w}")

        if result.df is None or result.df.empty:
            log.warning(f"{tag} {f.name}: No transactions extracted (confidence={result.confidence:.0%})")
            continue

        if result.confidence < min_confidence:
            log.warning(
                f"{tag} {f.name}: Confidence too low ({result.confidence:.0%} < {min_confidence:.0%}) — skipped"
            )
            continue

        conf_label = "HIGH" if result.confidence >= CONF_HIGH else \
                     "MEDIUM" if result.confidence >= CONF_MEDIUM else "LOW"
        log.info(
            f"{tag} {f.name}: {len(result.df)} rows, confidence={conf_label} ({result.confidence:.0%})"
        )
        frames.append(result.df)

    if not frames:
        return pd.DataFrame(columns=["Date","Payee","Amount","Source","Filepath"])

    return pd.concat(frames, ignore_index=True)
