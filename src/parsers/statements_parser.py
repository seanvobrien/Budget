"""
statements_parser.py  —  Universal bank/credit card statement parser
=====================================================================
Handles CSVs and PDFs from any institution by trying multiple strategies
in order of reliability. Designed to be extremely permissive — it would
rather capture a row with uncertain sign than drop it entirely.

Supported institutions (tested):
  Credit cards : Apple Card, Chase, Citi, Amex, Capital One, Discover, Barclays
  Debit/checking: Fidelity Cash Management, Chase, Bank of America, Wells Fargo
  Generic       : Any CSV with date + description + amount columns

Strategy per file type:
  CSV  → header sniff → column map → sign normalise
  PDF  → pdfplumber table extract (3 settings) → text regex (5 patterns) → merge

Output schema: Date (YYYY-MM-DD), Payee, Amount (positive=expense), Source, Filepath
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

CONF_HIGH   = 0.85
CONF_MEDIUM = 0.60
CONF_LOW    = 0.35

DATE_ALIASES = [
    "date", "transaction date", "trans date", "trans. date", "posted date",
    "post date", "settlement date", "settled date", "activity date",
    "effective date", "booking date", "value date", "process date",
]
PAYEE_ALIASES = [
    "description", "payee", "merchant", "payee name", "transaction description",
    "details", "narrative", "memo", "particulars", "name", "store name",
    "original description", "extended description", "transaction details",
    "reference", "remarks", "info",
]
AMOUNT_ALIASES = [
    "amount", "transaction amount", "amount (usd)", "amt", "debit/credit",
    "net amount", "sum", "value",
]
DEBIT_ALIASES  = ["debit", "withdrawals", "withdrawal", "charge", "charges",
                  "payments and credits", "money out", "out"]
CREDIT_ALIASES = ["credit", "deposits", "deposit", "credits", "payment",
                  "money in", "in"]

DATE_FORMATS = [
    "%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%d/%m/%Y", "%d/%m/%y",
    "%B %d, %Y", "%b %d, %Y", "%b %d %Y", "%B %d %Y",
    "%d %b %Y", "%d %B %Y", "%Y%m%d", "%m-%d-%Y", "%d-%m-%Y",
]

# Per-institution PDF text patterns — tried in order, first match wins
PATTERNS = [
    # Apple Card: "Jan 04, 2024   MERCHANT NAME   $123.45   2% $X.XX"
    re.compile(
        r"^(?P<date>[A-Za-z]{3}\s+\d{1,2},?\s+\d{4})"
        r"\s{2,}(?P<payee>.+?)\s{2,}"
        r"\$(?P<amount>[\d,]+\.\d{2})",
        re.IGNORECASE,
    ),
    # Chase / most banks: "01/15/2024   MERCHANT NAME   123.45"
    re.compile(
        r"^(?P<date>\d{1,2}/\d{1,2}(?:/\d{2,4})?)"
        r"\s{2,}(?P<payee>.+?)\s{2,}"
        r"(?P<amount>-?[\d,]+\.\d{2})\s*$",
        re.IGNORECASE,
    ),
    # Amex: "15 Jan 2024   MERCHANT NAME   123.45"
    re.compile(
        r"^(?P<date>\d{1,2}\s+[A-Za-z]{3}\s+\d{4})"
        r"\s{2,}(?P<payee>.+?)\s{2,}"
        r"(?P<amount>-?[\d,]+\.\d{2})",
        re.IGNORECASE,
    ),
    # Generic with $ sign: any date format, payee, $amount
    re.compile(
        r"(?P<date>"
        r"\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}"
        r"|\d{4}[/\-]\d{2}[/\-]\d{2}"
        r"|[A-Za-z]{3,9}\.?\s*\d{1,2},?\s*\d{4}"
        r")\s+(?P<payee>.+?)\s+\$(?P<amount>[\d,]+\.\d{2})",
        re.IGNORECASE,
    ),
    # Generic fallback: date + payee + bare number
    re.compile(
        r"(?P<date>"
        r"\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}"
        r"|\d{4}[/\-]\d{2}[/\-]\d{2}"
        r")\s+(?P<payee>[A-Z][A-Z\s\*\#\.\-\&\']{3,50})\s+"
        r"(?P<amount>-?[\d,]+\.\d{2})\s*$",
        re.IGNORECASE,
    ),
]

SKIP_RE = re.compile(
    r"^(page\s+\d|statement\s+(period|date|balance)|account\s+(number|summary)|"
    r"total|subtotal|balance|payment\s+due|minimum|opening|closing|"
    r"previous\s+balance|new\s+balance|credit\s+limit|available|reward|"
    r"thank\s+you|customer\s+service|\*+|^-+$)",
    re.IGNORECASE,
)


@dataclass
class ParseResult:
    df: Optional[pd.DataFrame]
    confidence: float
    method: str
    warnings: list[str] = field(default_factory=list)
    source_label: str = "Universal"


def _norm(c: str) -> str:
    return re.sub(r"\s+", " ", str(c).strip().lower())


def _find_col(headers: list[str], aliases: list[str]) -> Optional[str]:
    norm = {_norm(h): h for h in headers}
    for alias in aliases:
        if alias in norm:
            return norm[alias]
    for alias in aliases:
        for h_norm, h_orig in norm.items():
            if alias in h_norm or h_norm in alias:
                return h_orig
    return None


def _parse_date(val: str) -> Optional[str]:
    val = str(val).strip().rstrip("*").strip()
    if re.match(r"^\d{1,2}/\d{1,2}$", val):
        val = f"{val}/{datetime.today().year}"
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _parse_amount(val) -> Optional[float]:
    if val is None:
        return None
    s = re.sub(r"[$,\s]", "", str(val)).strip()
    if not s or s in ("--", "n/a", "na", "none", ""):
        return None
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    try:
        return float(s)
    except ValueError:
        return None


def _detect_sign_convention(df: pd.DataFrame, col: str) -> str:
    vals = pd.to_numeric(df[col], errors="coerce").dropna()
    if len(vals) == 0:
        return "positive=expense"
    neg_pct = (vals < 0).sum() / len(vals)
    return "positive=credit" if neg_pct > 0.6 else "positive=expense"


def _clean_payee(payee: str) -> str:
    payee = re.sub(r'\s+\d+%\s+\$[\d.]+\s*$', '', payee)  # Apple cashback
    payee = re.sub(r'[\*\|]+$', '', payee)
    return payee.strip()


def _build_df(rows, filepath, source, confidence, method, warnings):
    if not rows:
        return ParseResult(None, 0.0, method, warnings, source)
    df = pd.DataFrame(rows)
    df["Source"]   = source
    df["Filepath"] = filepath
    return ParseResult(df, confidence, method, warnings, source)


def _parse_csv(path: Path) -> ParseResult:
    warnings: list[str] = []
    source = path.stem
    try:
        raw = path.read_text(encoding="utf-8-sig", errors="replace").splitlines()
    except Exception as e:
        return ParseResult(None, 0.0, "csv-read", [f"Read error: {e}"], source)

    # Find header row
    header_idx = 0
    for i, line in enumerate(raw[:25]):
        parts = [p.strip().strip('"') for p in line.split(",")]
        non_num = sum(1 for p in parts if p and not re.match(r"^[\d\-\./\$\s]+$", p))
        if non_num >= 2:
            header_idx = i
            break

    try:
        df_raw = pd.read_csv(path, skiprows=header_idx, encoding="utf-8-sig",
                             on_bad_lines="skip", dtype=str, thousands=",")
    except Exception as e:
        return ParseResult(None, 0.0, "csv-table", [f"CSV parse error: {e}"], source)

    df_raw.columns = df_raw.columns.str.strip().str.replace('"', '')
    headers = list(df_raw.columns)

    date_col   = _find_col(headers, DATE_ALIASES)
    payee_col  = _find_col(headers, PAYEE_ALIASES)
    amount_col = _find_col(headers, AMOUNT_ALIASES)
    debit_col  = _find_col(headers, DEBIT_ALIASES)
    credit_col = _find_col(headers, CREDIT_ALIASES)

    if not date_col:
        return ParseResult(None, 0.0, "csv-table", ["No date column found"], source)
    if not payee_col:
        return ParseResult(None, 0.0, "csv-table", ["No payee column found"], source)
    if not amount_col and not debit_col and not credit_col:
        return ParseResult(None, 0.0, "csv-table", ["No amount column found"], source)

    cols_found = sum(x is not None for x in [date_col, payee_col,
                                              amount_col or debit_col or credit_col])
    confidence = {3: CONF_HIGH, 2: CONF_MEDIUM}.get(cols_found, CONF_LOW)

    convention = "positive=expense"
    if amount_col:
        convention = _detect_sign_convention(
            df_raw.assign(**{amount_col: pd.to_numeric(df_raw[amount_col], errors="coerce")}),
            amount_col
        )

    rows = []
    for _, row in df_raw.iterrows():
        date = _parse_date(row.get(date_col, ""))
        if not date:
            continue
        payee = str(row.get(payee_col, "")).strip()
        if not payee or payee.lower() in ("nan", "", "none"):
            continue

        amount = None
        if amount_col and pd.notna(row.get(amount_col)):
            amount = _parse_amount(row[amount_col])
            if amount is not None and convention == "positive=credit":
                amount = -amount
        elif debit_col or credit_col:
            d = _parse_amount(row.get(debit_col, "") or "") or 0.0
            c = _parse_amount(row.get(credit_col, "") or "") or 0.0
            if d or c:
                amount = d - c

        if amount is None:
            continue
        rows.append({"Date": date, "Payee": _clean_payee(payee), "Amount": amount})

    return _build_df(rows, str(path), source, confidence, "csv-table", warnings)


def _parse_pdf(path: Path) -> ParseResult:
    warnings: list[str] = []
    source = path.stem
    source_lower = source.lower()
    table_rows: list[dict] = []

    # Strategy 1: table extraction with multiple settings
    table_settings_list = [
        {},
        {"vertical_strategy": "lines",  "horizontal_strategy": "lines"},
        {"vertical_strategy": "text",   "horizontal_strategy": "text"},
    ]
    try:
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                for ts in table_settings_list:
                    try:
                        tables = page.extract_tables(ts) if ts else page.extract_tables()
                    except Exception:
                        continue
                    page_rows = []
                    for table in (tables or []):
                        if not table or len(table) < 2:
                            continue
                        hdr_idx = 0
                        for ri, row in enumerate(table[:6]):
                            if row and sum(1 for c in row if c and str(c).strip()) >= 2:
                                hdr_idx = ri
                                break
                        raw_hdrs = [str(c or "").strip() for c in table[hdr_idx]]
                        dc = _find_col(raw_hdrs, DATE_ALIASES)
                        pc = _find_col(raw_hdrs, PAYEE_ALIASES)
                        ac = _find_col(raw_hdrs, AMOUNT_ALIASES)
                        dbc = _find_col(raw_hdrs, DEBIT_ALIASES)
                        crc = _find_col(raw_hdrs, CREDIT_ALIASES)
                        if not (dc and pc):
                            continue
                        col_idx = {h: i for i, h in enumerate(raw_hdrs)}
                        def get(col, row):
                            idx = col_idx.get(col)
                            return str(row[idx]).strip() if idx is not None and idx < len(row) else ""
                        for row in table[hdr_idx + 1:]:
                            if not row or all(not str(c or "").strip() for c in row):
                                continue
                            date = _parse_date(get(dc, row))
                            if not date:
                                continue
                            payee = _clean_payee(get(pc, row))
                            if not payee or payee.lower() in ("nan", "none", ""):
                                continue
                            amount = None
                            if ac:
                                amount = _parse_amount(get(ac, row))
                            if amount is None and (dbc or crc):
                                d = _parse_amount(get(dbc, row) or "0") or 0.0
                                c = _parse_amount(get(crc, row) or "0") or 0.0
                                if d or c:
                                    amount = d - c
                            if amount is None:
                                continue
                            page_rows.append({"Date": date, "Payee": payee, "Amount": amount})
                    if page_rows:
                        table_rows.extend(page_rows)
                        break  # good result from this table setting, move to next page
    except Exception as e:
        warnings.append(f"Table extraction error: {e}")

    # Trust table results if we got enough (higher bar for Apple/Amex)
    min_trust = 10 if any(k in source_lower for k in ("apple", "amex", "american express")) else 3
    if len(table_rows) >= min_trust:
        return _build_df(table_rows, str(path), source, CONF_HIGH, "pdf-table", warnings)

    # Strategy 2: text line regex
    text_rows: list[dict] = []
    try:
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                for line in text.splitlines():
                    line = line.strip()
                    if not line or len(line) < 8 or SKIP_RE.match(line):
                        continue
                    for pattern in PATTERNS:
                        m = pattern.search(line)
                        if not m:
                            continue
                        date   = _parse_date(m.group("date"))
                        payee  = _clean_payee(m.group("payee"))
                        amount = _parse_amount(m.group("amount"))
                        if date and payee and amount is not None and len(payee) > 1:
                            text_rows.append({"Date": date, "Payee": payee, "Amount": amount})
                            break
    except Exception as e:
        warnings.append(f"Text extraction error: {e}")

    # Merge table partial + text rows, dedup by date+amount
    seen = set()
    combined = []
    for r in text_rows + table_rows:
        key = (r["Date"], round(abs(r["Amount"]), 2))
        if key not in seen:
            seen.add(key)
            combined.append(r)

    if not combined:
        warnings.append("Zero rows extracted — may be scanned/image PDF")
        return ParseResult(None, 0.0, "pdf-none", warnings, source)

    method = "pdf-table+text" if table_rows and text_rows else \
             "pdf-table" if table_rows else "pdf-text"
    conf   = CONF_MEDIUM if len(combined) > 5 else CONF_LOW
    return _build_df(combined, str(path), source, conf, method, warnings)


def parse_file(path: Path) -> ParseResult:
    ext = path.suffix.lower()
    if ext == ".csv":
        return _parse_csv(path)
    elif ext == ".pdf":
        return _parse_pdf(path)
    return ParseResult(None, 0.0, "unsupported", [f"Unsupported: {ext}"], path.stem)


def parse_directory(directory: Path, min_confidence: float = CONF_LOW) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    files = sorted([
        f for f in directory.iterdir()
        if f.suffix.lower() in (".csv", ".pdf") and not f.name.startswith(".")
    ])

    if not files:
        log.info(f"[StatementsParser] No files in {directory}")
        return pd.DataFrame(columns=["Date", "Payee", "Amount", "Source", "Filepath"])

    for f in files:
        result = parse_file(f)
        for w in (result.warnings or []):
            log.debug(f"[StatementsParser:{result.method}] {f.name}: {w}")
        if result.df is None or result.df.empty:
            log.warning(f"[StatementsParser:{result.method}] {f.name}: 0 rows")
            continue
        if result.confidence < min_confidence:
            log.warning(f"[StatementsParser:{result.method}] {f.name}: "
                        f"confidence {result.confidence:.0%} < threshold — skipped")
            continue
        label = ("HIGH" if result.confidence >= CONF_HIGH else
                 "MEDIUM" if result.confidence >= CONF_MEDIUM else "LOW")
        log.info(f"[StatementsParser:{result.method}] {f.name}: "
                 f"{len(result.df)} rows, confidence={label} ({result.confidence:.0%})")
        frames.append(result.df)

    if not frames:
        return pd.DataFrame(columns=["Date", "Payee", "Amount", "Source", "Filepath"])
    return pd.concat(frames, ignore_index=True)
