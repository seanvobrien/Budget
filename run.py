from __future__ import annotations
from pathlib import Path
import pandas as pd

from src.io_utils import ensure_dirs, write_transactions_csv, normalize_and_enrich
from src.mapping import apply_mapping, MAPPING
from src.parsers.statements_parser import parse_directory, CONF_LOW
from src.parsers.investments_parser import parse_investments_csv_dir
from src.io_utils import write_portfolio_csv

# ── Statement folder names ───────────────────────────────────────────────────
CREDIT_DIR   = "credit"
DEBIT_DIR    = "debit"
INVEST_DIR   = "investments"


def _find_stmts_root(base: Path) -> Path:
    for name in ("statements", "Statements"):
        p = base / name
        if p.exists():
            return p
    return base / "Statements"


def main(base: Path = None):
    if base is None:
        base = Path(".")
    paths = ensure_dirs(base)
    root  = _find_stmts_root(base)

    frames: list[pd.DataFrame] = []

    # ── 1. Credit (statements_parser) ────────────────────────────────────────
    credit_dir = root / CREDIT_DIR
    if credit_dir.exists():
        df_credit = parse_directory(credit_dir, min_confidence=CONF_LOW)
        print(f"[Debug] Credit rows: {len(df_credit)}")
        if not df_credit.empty:
            frames.append(df_credit)
    else:
        print(f"[Warn]  {credit_dir} not found — skipping Credit")

    # ── 2. Debit (statements_parser) — CSV and PDF ───────────────────────────
    debit_dir = root / DEBIT_DIR
    if debit_dir.exists():
        df_debit = parse_directory(debit_dir, min_confidence=CONF_LOW)
        print(f"[Debug] Debit rows: {len(df_debit)}")
        if not df_debit.empty:
            frames.append(df_debit)
    else:
        print(f"[Warn]  {debit_dir} not found — skipping Debit")

    # ── 3. Combine ────────────────────────────────────────────────────────────
    if frames:
        df = pd.concat(frames, ignore_index=True)
    else:
        df = pd.DataFrame(columns=["Date", "Payee", "Amount", "Source", "Filepath"])

    print(f"[Debug] After parse: {len(df)} rows")

    # ── 4. Normalize & enrich ─────────────────────────────────────────────────
    df = normalize_and_enrich(df)
    print(f"[Debug] After normalize/enrich: {len(df)} rows")

    # ── 5. Force abs for utilities ────────────────────────────────────────────
    mask_util = df["Payee"].str.contains(
        "National Grid|NGRID06|Eversource", case=False, na=False
    )
    df.loc[mask_util, "Amount"] = df.loc[mask_util, "Amount"].abs()
    print(f"[Debug] Forced absolute for {mask_util.sum()} utility rows")

    # ── 6. Category mapping ───────────────────────────────────────────────────
    df = apply_mapping(df, MAPPING)
    if "Category" not in df.columns:
        df["Category"] = "All-Other"

    # ── 7. Deduplicate ────────────────────────────────────────────────────────
    df["Concat"] = (
        df["Date"].astype(str).str.strip() + "|" +
        df["Payee"].astype(str).str.strip() + "|" +
        df["Amount"].astype(str).str.strip()
    )
    before = len(df)
    df = df.drop_duplicates(subset=["Concat"]).drop(columns=["Concat"])
    print(f"[Debug] Deduped: {before} → {len(df)} rows")

    # ── 8. Write transactions CSV ─────────────────────────────────────────────
    out = write_transactions_csv(df.sort_values("Date"), paths["output"])
    print(f"[OK] Wrote {out} with {len(df):,} rows")

    # ── 9. Investments (investments_parser) ───────────────────────────────────
    invest_dir = root / INVEST_DIR
    df_invest = parse_investments_csv_dir(invest_dir)
    print(f"[Debug] Investments rows: {len(df_invest)}")
    if not df_invest.empty:
        out_invest = write_portfolio_csv(df_invest, paths["output"])
        print(f"[OK] Wrote {out_invest} with {len(df_invest):,} rows")


if __name__ == "__main__":
    main()
