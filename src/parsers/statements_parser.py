"""
statements_parser.py  —  Universal bank/credit card statement parser
=====================================================================
Handles CSVs and PDFs from 20+ institutions by using:

  1. Institution-specific CSV column maps (exact headers per bank)
  2. Generic column sniffing (any bank we haven't seen before)
  3. Multi-strategy PDF extraction (tables × 3 settings + text × 5 patterns)
  4. Groq LLM fallback — last resort if everything else fails
     (reads groq_api_key from settings.json, no setup needed for users)

Confirmed institutions:
  Apple Card, Chase CC, Chase Checking, Fidelity Checking, Citi, Amex,
  Capital One, Bank of America, Wells Fargo, Discover, US Bank, Barclays

Output: Date (YYYY-MM-DD), Payee, Amount (positive=expense), Source, Filepath
"""
from __future__ import annotations
import re, json, logging, os, urllib.request, urllib.error
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd
import pdfplumber

log = logging.getLogger(__name__)

# ── Confidence levels ─────────────────────────────────────────────────────────
CONF_HIGH   = 0.85
CONF_MEDIUM = 0.60
CONF_LOW    = 0.35

# ── Date formats ──────────────────────────────────────────────────────────────
DATE_FORMATS = [
    "%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%d/%m/%Y", "%d/%m/%y",
    "%B %d, %Y", "%b %d, %Y", "%b %d %Y", "%B %d %Y",
    "%d %b %Y", "%d %B %Y", "%Y%m%d", "%m-%d-%Y", "%d-%m-%Y",
    "%m/%d",  # Chase/Capital One CC — no year, infer below
]

# ─────────────────────────────────────────────────────────────────────────────
# INSTITUTION-SPECIFIC CSV SCHEMAS
# Each entry: { "date": col, "payee": col, "amount": col | None,
#               "debit": col | None, "credit": col | None,
#               "sign": "normal"|"inverted",   # normal=positive is expense
#               "skip_rows": int }             # header disclaimer rows
# ─────────────────────────────────────────────────────────────────────────────
BANK_CSV_SCHEMAS = {

    # ── Apple Card ────────────────────────────────────────────────────────────
    # CSV from Wallet app: Transaction Date, Clearing Date, Description, Merchant,
    #   Merchant ID, Category, Buy Price, Amount (USD), Purchased By
    "apple_card": {
        "match": ["Transaction Date", "Description", "Amount (USD)"],
        "match_any": [
            ["Transaction Date", "Description", "Merchant", "Amount (USD)"],
            ["Transaction Date", "Description", "Amount (USD)"],
            ["Transaction Date", "Clearing Date", "Description", "Amount (USD)"],
        ],
        "date":        "Transaction Date",
        "payee":       "Merchant",          # cleaner than Description
        "payee_fallbacks": ["Description"],  # if Merchant blank
        "amount":      "Amount (USD)",
        "amount_fallbacks": ["Amount"],
        "sign":        "normal",            # positive = expense
    },

    # ── Chase Credit Card ─────────────────────────────────────────────────────
    # Transaction Date, Post Date, Description, Category, Type, Amount, Memo
    "chase_cc": {
        "match": ["Transaction Date", "Post Date", "Description", "Amount"],
        "match_any": [
            ["Transaction Date", "Post Date", "Description", "Category", "Type", "Amount"],
            ["Transaction Date", "Post Date", "Description", "Amount"],
            ["Transaction Date", "Description", "Category", "Amount"],
        ],
        "date":        "Transaction Date",
        "payee":       "Description",
        "payee_fallbacks": ["Merchant Name", "Memo"],
        "amount":      "Amount",
        "sign":        "normal",            # Chase CC: positive = expense
    },

    # ── Chase Checking ────────────────────────────────────────────────────────
    # Details, Posting Date, Description, Amount, Type, Balance, Check or Slip #
    "chase_checking": {
        "match": ["Details", "Posting Date", "Description", "Amount"],
        "match_any": [
            ["Details", "Posting Date", "Description", "Amount", "Type", "Balance"],
            ["Details", "Posting Date", "Description", "Amount"],
        ],
        "date":        "Posting Date",
        "date_fallbacks": ["Date"],
        "payee":       "Description",
        "amount":      "Amount",
        "sign":        "inverted",          # positive=deposit → flip so expense=positive
    },

    # ── Fidelity Cash Management Checking ────────────────────────────────────
    # Run Date, Account, Action, Symbol, Description, Type, Quantity,
    # Price ($), Commission ($), Amount ($), Settlement Date
    "fidelity_checking": {
        "match": ["Run Date", "Account", "Action", "Amount ($)"],
        "date":        "Run Date",
        "payee":       "Description",       # merchant name in Description when populated
        "payee_fallbacks": ["Action"],      # fallback: "DIRECT DEBIT", "ATM", etc.
        "amount":      "Amount ($)",
        "sign":        "normal",            # parentheses = expense, already handled
        "account_filter": "CHECKING",       # exclude brokerage rows
    },

    # ── Fidelity History (brokerage — excluded from debit/credit) ─────────────
    "fidelity_history": {
        "match": ["Run Date", "Account", "Action", "Symbol", "Amount ($)"],
        "date":        "Run Date",
        "payee":       "Description",
        "payee_fallbacks": ["Action"],
        "amount":      "Amount ($)",
        "sign":        "normal",
        "account_filter": "CHECKING",
        "investment_only": True,
    },

    # ── Citi Credit Card ──────────────────────────────────────────────────────
    # Date, Description, Debit, Credit  OR  Date, Description, Amount
    "citi_cc": {
        "match": ["Date", "Description", "Debit", "Credit"],
        "match_any": [
            ["Date", "Description", "Debit", "Credit"],
            ["Date", "Description", "Amount", "Member Name"],  # Citi v2
        ],
        "date":        "Date",
        "date_fallbacks": ["Transaction Date"],
        "payee":       "Description",
        "debit":       "Debit",
        "credit":      "Credit",
        "amount":      "Amount",            # fallback if no split cols
        "sign":        "normal",
    },

    # ── American Express ──────────────────────────────────────────────────────
    # Date, Description, Amount   (Amount: positive = charge, negative = payment)
    "amex": {
        "match": ["Date", "Description", "Amount"],
        "match_any": [
            ["Date", "Description", "Amount", "Extended Details"],
            ["Date", "Description", "Amount"],
        ],
        "date":        "Date",
        "payee":       "Description",
        "payee_fallbacks": ["Merchant", "Appears On Your Statement As"],
        "amount":      "Amount",
        "sign":        "normal",            # CORRECT: Amex CSV positive=charge (expense)
    },

    # ── American Express Extended ─────────────────────────────────────────────
    # Date, Description, Amount, Extended Details, Appears On Your Statement As,
    # Address, City/State, Zip, Country, Reference, Category
    "amex_extended": {
        "match": ["Date", "Description", "Amount", "Extended Details"],
        "date":        "Date",
        "payee":       "Appears On Your Statement As",
        "payee_fallbacks": ["Description", "Merchant"],
        "amount":      "Amount",
        "sign":        "normal",            # positive = charge
    },

    # ── Capital One Credit Card ───────────────────────────────────────────────
    # Transaction Date, Posted Date, Card No., Description, Category, Debit, Credit
    "capital_one_cc": {
        "match": ["Transaction Date", "Posted Date", "Description", "Debit", "Credit"],
        "match_any": [
            ["Transaction Date", "Posted Date", "Card No.", "Description", "Debit", "Credit"],
            ["Transaction Date", "Posted Date", "Description", "Debit", "Credit"],
            ["Transaction Date", "Posted Date", "Card Number", "Description", "Debit", "Credit"],
        ],
        "date":        "Transaction Date",
        "payee":       "Description",
        "debit":       "Debit",
        "credit":      "Credit",
        "sign":        "normal",
    },

    # ── Capital One Checking ──────────────────────────────────────────────────
    # Transaction Date, Transaction Type, Transaction Description,
    # Debit, Credit, Balance
    "capital_one_checking": {
        "match": ["Transaction Date", "Transaction Type", "Transaction Description",
                  "Debit", "Credit"],
        "date":        "Transaction Date",
        "payee":       "Transaction Description",
        "payee_fallbacks": ["Description"],
        "debit":       "Debit",
        "credit":      "Credit",
        "sign":        "normal",
    },

    # ── Bank of America Checking ──────────────────────────────────────────────
    # Date, Description, Amount, Running Bal.
    "bofa": {
        "match": ["Date", "Description", "Amount"],
        "match_any": [
            ["Date", "Description", "Amount", "Running Bal."],
            ["Date", "Description", "Amount", "Running Bal"],   # no period variant
        ],
        "date":        "Date",
        "payee":       "Description",
        "amount":      "Amount",
        "sign":        "inverted",          # BofA: negative=expense positive=deposit
    },

    # ── Bank of America Credit Card ───────────────────────────────────────────
    # Posted Date, Reference Number, Payee, Address, Amount
    "bofa_cc": {
        "match": ["Posted Date", "Payee", "Amount"],
        "match_any": [
            ["Posted Date", "Reference Number", "Payee", "Address", "Amount"],
            ["Posted Date", "Payee", "Amount"],
            ["Transaction Date", "Payee", "Amount"],
        ],
        "date":        "Posted Date",
        "date_fallbacks": ["Transaction Date", "Date"],
        "payee":       "Payee",
        "payee_fallbacks": ["Description"],
        "amount":      "Amount",
        "sign":        "inverted",          # BofA CC: negative=charge positive=payment
    },

    # ── Wells Fargo (positional — no headers) ─────────────────────────────────
    # Checking: Date, Amount, *, Description, Balance (5 cols, no header row)
    # Credit:   Date, Amount, *, Description (4 cols)
    "wells_fargo": {
        "match":      [],
        "positional": True,
        "sign":       "inverted",           # WF: negative=debit positive=deposit
        "positional_map": {
            5: {"date": 0, "amount": 1, "payee": 4},   # checking: bal in col 4
            4: {"date": 0, "amount": 1, "payee": 3},   # credit card
        },
    },

    # ── Discover Credit Card ──────────────────────────────────────────────────
    # Trans. Date, Post Date, Description, Amount, Category
    "discover": {
        "match": ["Description", "Amount"],
        "match_any": [
            ["Trans. Date", "Post Date", "Description", "Amount", "Category"],
            ["Transaction Date", "Post Date", "Description", "Amount", "Category"],
            ["Trans. Date", "Post Date", "Description", "Amount"],
        ],
        "date":        "Trans. Date",
        "date_fallbacks": ["Transaction Date", "Date"],
        "payee":       "Description",
        "amount":      "Amount",
        "sign":        "normal",            # Discover CSV: positive=charge negative=payment
    },

    # ── US Bank ───────────────────────────────────────────────────────────────
    # Date, Transaction, Name, Memo, Amount
    "us_bank": {
        "match": ["Date", "Transaction", "Name", "Memo", "Amount"],
        "date":        "Date",
        "payee":       "Name",
        "payee_fallbacks": ["Memo", "Transaction"],  # Name is often blank
        "amount":      "Amount",
        "sign":        "inverted",          # US Bank: negative=debit
    },

    # ── Barclays ──────────────────────────────────────────────────────────────
    # Transaction Date, Transaction Type, Sort Code, Account Number,
    # Transaction Description, Debit Amount, Credit Amount, Balance
    "barclays": {
        "match": ["Transaction Date", "Transaction Description", "Debit Amount", "Credit Amount"],
        "match_any": [
            ["Transaction Date", "Transaction Type", "Sort Code",
             "Account Number", "Transaction Description", "Debit Amount", "Credit Amount"],
            ["Transaction Date", "Transaction Description", "Debit Amount", "Credit Amount"],
        ],
        "date":        "Transaction Date",
        "payee":       "Transaction Description",
        "payee_fallbacks": ["Transaction Type"],
        "debit":       "Debit Amount",
        "credit":      "Credit Amount",
        "sign":        "normal",
    },

    # ── Charles Schwab Checking ───────────────────────────────────────────────
    # "Date","Type","Check #","Description","Withdrawal (-)","Deposit (+)","RunningBalance"
    # Newer format may drop parens: "Withdrawal","Deposit"
    "schwab_checking": {
        "match": ["Date", "Description"],
        "match_any": [
            ["Date", "Type", "Check #", "Description", "Withdrawal (-)", "Deposit (+)"],
            ["Date", "Type", "Description", "Withdrawal (-)", "Deposit (+)"],
            ["Date", "Type", "Description", "Withdrawal", "Deposit"],
        ],
        "date":        "Date",
        "payee":       "Description",
        "debit":       "Withdrawal (-)",
        "debit_fallbacks": ["Withdrawal"],
        "credit":      "Deposit (+)",
        "credit_fallbacks": ["Deposit"],
        "sign":        "normal",
    },

    # ── Charles Schwab Brokerage (investment — excluded from spending) ─────────
    # Date, Action, Symbol, Description, Quantity, Price ($), Fees ($), Amount ($)
    "schwab_brokerage": {
        "match": ["Date", "Action", "Symbol", "Description", "Quantity", "Price ($)", "Amount ($)"],
        "date":        "Date",
        "payee":       "Description",
        "payee_fallbacks": ["Action"],
        "amount":      "Amount ($)",
        "sign":        "inverted",
        "investment_only": True,
    },

    # ── PNC Checking ──────────────────────────────────────────────────────────
    # Date, Description, Withdrawals, Deposits, Balance
    "pnc_checking": {
        "match": ["Date", "Description", "Withdrawals", "Deposits"],
        "match_any": [
            ["Date", "Description", "Withdrawals", "Deposits", "Balance"],
            ["Date", "Description", "Withdrawals", "Deposits"],
        ],
        "date":        "Date",
        "payee":       "Description",
        "debit":       "Withdrawals",
        "debit_fallbacks": ["Withdrawal"],
        "credit":      "Deposits",
        "credit_fallbacks": ["Deposit"],
        "sign":        "normal",
    },

    # ── PNC Credit Card ───────────────────────────────────────────────────────
    # Transaction Date, Posted Date, Description, Amount, Transaction Type
    "pnc_cc": {
        "match": ["Transaction Date", "Posted Date", "Description", "Amount"],
        "date":        "Transaction Date",
        "payee":       "Description",
        "amount":      "Amount",
        "sign":        "normal",            # PNC CC: positive=charge
    },

    # ── TD Bank ───────────────────────────────────────────────────────────────
    # Date, Description, Debit, Credit, Balance
    # Or: Account Number, Date, Description, Debit, Credit, Balance
    "td_bank": {
        "match": ["Date", "Description", "Debit", "Credit", "Balance"],
        "match_any": [
            ["Account Number", "Date", "Description", "Debit", "Credit", "Balance"],
            ["Date", "Description", "Debit", "Credit", "Balance"],
            ["Date", "Description", "Type", "Debit", "Credit", "Balance"],
        ],
        "date":        "Date",
        "payee":       "Description",
        "debit":       "Debit",
        "credit":      "Credit",
        "sign":        "normal",
    },

    # ── Ally Bank ─────────────────────────────────────────────────────────────
    # Date, Time, Amount, Type, Description
    "ally_bank": {
        "match": ["Date", "Amount", "Type", "Description"],
        "match_any": [
            ["Date", "Time", "Amount", "Type", "Description"],
            ["Date", "Amount", "Type", "Description"],
        ],
        "date":        "Date",
        "payee":       "Description",
        "payee_fallbacks": ["Type"],
        "amount":      "Amount",
        "sign":        "normal",            # Ally: positive=expense negative=deposit
    },

    # ── Navy Federal Credit Union ─────────────────────────────────────────────
    # Transaction Date, Amount, Description, Balance, Memo
    "navy_federal": {
        "match": ["Transaction Date", "Amount", "Description", "Balance"],
        "match_any": [
            ["Transaction Date", "Amount", "Description", "Balance", "Memo"],
            ["Transaction Date", "Amount", "Description", "Balance"],
        ],
        "date":        "Transaction Date",
        "payee":       "Description",
        "payee_fallbacks": ["Memo"],
        "amount":      "Amount",
        "sign":        "inverted",          # NFCU: negative=debit
    },

    # ── USAA ─────────────────────────────────────────────────────────────────
    # Date, Description, Original Description, Category, Amount, Status
    "usaa": {
        "match": ["Date", "Description", "Original Description", "Amount"],
        "match_any": [
            ["Date", "Description", "Original Description", "Category", "Amount", "Status"],
            ["Date", "Description", "Original Description", "Amount"],
        ],
        "date":        "Date",
        "payee":       "Description",
        "payee_fallbacks": ["Original Description"],
        "amount":      "Amount",
        "sign":        "normal",            # USAA: positive=expense
    },

    # ── PayPal ────────────────────────────────────────────────────────────────
    # Date, Time, TimeZone, Name, Type, Status, Currency, Gross, Fee, Net,
    # From Email, To Email, Transaction ID, ...
    "paypal": {
        "match": ["Date", "Name", "Gross", "Currency"],
        "match_any": [
            ["Date", "Time", "TimeZone", "Name", "Type", "Status", "Currency", "Gross"],
            ["Date", "Name", "Type", "Status", "Currency", "Gross"],
            ["Date", "Name", "Gross"],
        ],
        "date":        "Date",
        "payee":       "Name",
        "payee_fallbacks": ["Type"],
        "amount":      "Gross",
        "amount_fallbacks": ["Net"],
        "sign":        "inverted",          # PayPal: negative=you sent (expense)
        "status_filter": "Completed",       # skip pending/reversed
        "currency_filter": "USD",           # skip foreign currency rows
    },

    # ── Mint (Intuit) ─────────────────────────────────────────────────────────
    # Date, Description, Original Description, Amount, Transaction Type,
    # Category, Account Name, Labels, Notes
    "mint": {
        "match": ["Date", "Description", "Original Description", "Transaction Type"],
        "match_any": [
            ["Date", "Description", "Original Description", "Amount",
             "Transaction Type", "Category", "Account Name"],
            ["Date", "Description", "Amount", "Transaction Type"],
        ],
        "date":        "Date",
        "payee":       "Description",
        "payee_fallbacks": ["Original Description"],
        "amount":      "Amount",
        "sign":        "normal",
        "type_col":    "Transaction Type",  # "debit"/"credit" determines sign
    },

    # ── Venmo ─────────────────────────────────────────────────────────────────
    # ID, Datetime, Type, Status, Note, From, To, Amount (total),
    # Amount (tip), Amount (tax), Amount (fee)
    "venmo": {
        "match": ["Datetime", "Note", "Amount (total)"],
        "match_any": [
            ["ID", "Datetime", "Type", "Status", "Note", "From", "To", "Amount (total)"],
            ["Datetime", "Note", "Amount (total)"],
        ],
        "date":        "Datetime",
        "payee":       "Note",
        "payee_fallbacks": ["To", "From"],  # Note is often blank
        "amount":      "Amount (total)",
        "amount_fallbacks": ["Amount"],
        "sign":        "inverted",          # Venmo: negative=you paid positive=received
        "status_filter": "Complete",        # skip pending/failed
        "strip_currency": True,             # amounts come as "+ $5.00" or "- $5.00"
    },

    # ── Truist (SunTrust / BB&T) ──────────────────────────────────────────────
    "truist": {
        "match": ["Date", "Amount", "Description", "Running Balance"],
        "match_any": [
            ["Date", "Transaction Type", "Amount", "Description", "Running Balance"],
            ["Date", "Amount", "Description", "Running Balance"],
            ["Date", "Transaction Description", "Debit", "Credit", "Balance"],  # BB&T
        ],
        "date":        "Date",
        "payee":       "Description",
        "payee_fallbacks": ["Transaction Description", "Transaction Type"],
        "amount":      "Amount",
        "sign":        "inverted",          # Truist: negative=debit
    },

    # ── KeyBank ───────────────────────────────────────────────────────────────
    # Date, Transaction Amount, Description
    "keybank": {
        "match": ["Date", "Transaction Amount", "Description"],
        "date":        "Date",
        "payee":       "Description",
        "amount":      "Transaction Amount",
        "amount_fallbacks": ["Amount"],
        "sign":        "inverted",
    },

    # ── Regions Bank ─────────────────────────────────────────────────────────
    # Date, Transaction Type, Number, Payee, Memo, Amount, Balance
    "regions": {
        "match": ["Date", "Payee", "Amount", "Balance"],
        "match_any": [
            ["Date", "Transaction Type", "Number", "Payee", "Memo", "Amount", "Balance"],
            ["Date", "Payee", "Amount", "Balance"],
        ],
        "date":        "Date",
        "payee":       "Payee",
        "payee_fallbacks": ["Description", "Memo"],
        "amount":      "Amount",
        "sign":        "inverted",
    },

    # ── Huntington Bank ───────────────────────────────────────────────────────
    # Date, Transaction, Description, Deposits, Withdrawals, Balance
    "huntington": {
        "match": ["Date", "Description", "Deposits", "Withdrawals"],
        "match_any": [
            ["Date", "Transaction", "Description", "Deposits", "Withdrawals", "Balance"],
            ["Date", "Description", "Deposits", "Withdrawals"],
            ["Date", "Description", "Deposit", "Withdrawal", "Balance"],  # singular
        ],
        "date":        "Date",
        "payee":       "Description",
        "debit":       "Withdrawals",
        "debit_fallbacks": ["Withdrawal"],
        "credit":      "Deposits",
        "credit_fallbacks": ["Deposit"],
        "sign":        "normal",
    },

    # ── M&T Bank ──────────────────────────────────────────────────────────────
    # Date Posted, Transaction Amount, Description, Check Number
    "mt_bank": {
        "match": ["Transaction Amount", "Description"],
        "match_any": [
            ["Date Posted", "Transaction Amount", "Description", "Check Number"],
            ["Date Posted", "Transaction Amount", "Description"],
            ["Post Date", "Transaction Amount", "Description"],
            ["Posted Date", "Transaction Amount", "Description"],
        ],
        "date":        "Date Posted",
        "date_fallbacks": ["Post Date", "Posted Date", "Date"],
        "payee":       "Description",
        "amount":      "Transaction Amount",
        "amount_fallbacks": ["Amount"],
        "sign":        "inverted",
    },

    # ── Citizens Bank ─────────────────────────────────────────────────────────
    # Date, Description, Debit (-), Credit (+), Balance
    "citizens_bank": {
        "match": ["Date", "Description"],
        "match_any": [
            ["Date", "Description", "Debit (-)", "Credit (+)", "Balance"],
            ["Date", "Description", "Debit", "Credit", "Balance"],   # without parens
        ],
        "date":        "Date",
        "payee":       "Description",
        "debit":       "Debit (-)",
        "debit_fallbacks": ["Debit"],
        "credit":      "Credit (+)",
        "credit_fallbacks": ["Credit"],
        "sign":        "normal",
    },

    # ── Fifth Third Bank ─────────────────────────────────────────────────────
    # Date, Transaction Description, Amount, Balance
    "fifth_third": {
        "match": ["Date", "Transaction Description", "Amount"],
        "match_any": [
            ["Date", "Transaction Description", "Amount", "Balance"],
            ["Date", "Transaction Description", "Amount"],
            ["Date", "Description", "Amount", "Balance"],           # alternate
        ],
        "date":        "Date",
        "payee":       "Transaction Description",
        "payee_fallbacks": ["Description"],
        "amount":      "Amount",
        "sign":        "inverted",
    },

    # ── SoFi Bank ─────────────────────────────────────────────────────────────
    # Transaction Date, Description, Amount, Status, Reference Number
    "sofi": {
        "match": ["Transaction Date", "Description", "Amount", "Status"],
        "match_any": [
            ["Transaction Date", "Description", "Amount", "Status", "Reference Number"],
            ["Transaction Date", "Description", "Amount", "Status"],
        ],
        "date":        "Transaction Date",
        "payee":       "Description",
        "amount":      "Amount",
        "sign":        "inverted",          # SoFi: negative=expense
    },

    # ── Chime ─────────────────────────────────────────────────────────────────
    # Date, Description, Category, Amount, Balance
    "chime": {
        "match": ["Date", "Description", "Category", "Amount", "Balance"],
        "date":        "Date",
        "payee":       "Description",
        "amount":      "Amount",
        "sign":        "normal",            # Chime: positive=expense negative=deposit
    },

    # ── Simple Bank (closed 2021) ─────────────────────────────────────────────
    # Date, Memo, Amount, Running Balance
    "simple_bank": {
        "match": ["Date", "Memo", "Amount", "Running Balance"],
        "date":        "Date",
        "payee":       "Memo",
        "payee_fallbacks": ["Description"],
        "amount":      "Amount",
        "sign":        "inverted",
    },
}


# ── Generic column aliases (fallback when no schema matches) ──────────────────
DATE_ALIASES = [
    "date", "transaction date", "trans date", "trans. date", "posted date",
    "post date", "settlement date", "settled date", "activity date",
    "effective date", "booking date", "value date", "process date",
    "run date", "posting date",
]
PAYEE_ALIASES = [
    "description", "payee", "merchant", "payee name", "transaction description",
    "details", "narrative", "memo", "particulars", "name", "store name",
    "original description", "extended description", "transaction details",
    "reference", "remarks", "info", "transaction", "transaction type",
]
AMOUNT_ALIASES = [
    "amount", "transaction amount", "amount (usd)", "amt", "debit/credit",
    "net amount", "sum", "value", "amount ($)",
]
DEBIT_ALIASES  = ["debit", "debit amount", "withdrawals", "withdrawal",
                  "charge", "charges", "money out"]
CREDIT_ALIASES = ["credit", "credit amount", "deposits", "deposit",
                  "credits", "payment", "money in"]

# ── PDF text patterns ─────────────────────────────────────────────────────────
PDF_PATTERNS = [
    # Apple Card: "Jan 04, 2024   MERCHANT   $123.45   2% $0.11"
    # The REAL amount is the LAST dollar figure on the line (cashback % comes after).
    # Anchor the amount match to end-of-line so we grab the final number, then
    # strip the cashback portion from the payee.
    re.compile(
        r"^(?P<date>[A-Za-z]{3}\s+\d{1,2},?\s+\d{4})"
        r"\s+(?P<payee>.+?)\s+"
        r"[-\u2212]?\$?(?P<amount>[\d,]+\.\d{2})\s*$",
        re.IGNORECASE,
    ),
    # Chase CC PDF: "01/15  MERCHANT NAME  123.45" (no year in CC PDF)
    re.compile(
        r"^(?P<date>\d{1,2}/\d{1,2}(?:/\d{2,4})?)"
        r"\s{2,}(?P<payee>.+?)\s{2,}"
        r"(?P<amount>-?[\d,]+\.\d{2})\s*$",
        re.IGNORECASE,
    ),
    # Amex: "15 Jan 2024   MERCHANT   123.45"
    re.compile(
        r"^(?P<date>\d{1,2}\s+[A-Za-z]{3}\s+\d{4})"
        r"\s{2,}(?P<payee>.+?)\s{2,}"
        r"(?P<amount>-?[\d,]+\.\d{2})\s*$",
        re.IGNORECASE,
    ),
    # Schwab / PNC / TD Bank: "01/15/2024  Description  Amount  Balance"
    # Amount before running balance at end of line
    re.compile(
        r"^(?P<date>\d{1,2}/\d{1,2}/\d{2,4})\s+"
        r"(?P<payee>.+?)\s+"
        r"(?P<amount>-?[\d,]+\.\d{2})\s+"
        r"-?[\d,]+\.\d{2}\s*$",
        re.IGNORECASE,
    ),
    # Schwab/checking: "01/15/2024  DIRECT DEPOSIT  5,200.00" end-anchored
    re.compile(
        r"^(?P<date>\d{1,2}/\d{1,2}/\d{2,4})\s+"
        r"(?P<payee>[A-Z][A-Z0-9\s\*\#\.\-\&\'\/\,]{3,70}?)\s+"
        r"(?P<amount>-?[\d,]+\.\d{2})\s*$",
        re.IGNORECASE,
    ),
    # Generic with $ sign: any date format, payee, $amount — end-anchored
    re.compile(
        r"(?P<date>"
        r"\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}"
        r"|\d{4}[/\-]\d{2}[/\-]\d{2}"
        r"|[A-Za-z]{3,9}\.?\s*\d{1,2},?\s*\d{4}"
        r")\s+(?P<payee>.+?)\s+\$(?P<amount>[\d,]+\.\d{2})\s*$",
        re.IGNORECASE,
    ),
    # Generic bare number — most permissive, last resort, end-anchored
    re.compile(
        r"(?P<date>\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}|\d{4}[/\-]\d{2}[/\-]\d{2})"
        r"\s+(?P<payee>[A-Z][A-Z0-9\s\*\#\.\-\&\'\/]{3,60})\s+"
        r"(?P<amount>-?[\d,]+\.\d{2})\s*$",
        re.IGNORECASE,
    ),
]

SKIP_RE = re.compile(
    r"^(page\s+\d|statement\s+(period|date|balance|ending)|account\s+(number|summary|type)|"
    r"total|subtotal|balance\s+(forward|due|summary)|payment\s+due|minimum|opening|closing|"
    r"previous\s+balance|new\s+balance|credit\s+limit|available\s+(credit|balance)|reward|"
    r"thank\s+you|customer\s+(service|care)|beginning\s+balance|ending\s+balance|"
    r"daily\s+(ending\s+)?balance|annual\s+percentage|interest\s+(rate|charge|charged)|"
    r"fees\s+charged|cash\s+advance|standard\s+purchase|summary\s+of|"
    r"\*+|^-{3,}$|^\s*$|^[=_]{3,}$)",
    re.IGNORECASE,
)

# Section headers that indicate transaction type in Wells Fargo / BofA / PNC style PDFs
# We track these to set sign for following lines
DEPOSIT_SECTION_RE   = re.compile(r"^(deposits?\s+(and|&)\s+|electronic\s+deposits?|credits?\s+and|other\s+credits?)", re.IGNORECASE)
WITHDRAWAL_SECTION_RE = re.compile(r"^(electronic\s+withdrawals?|debit\s+card|checks?\s+paid|other\s+withdrawals?|withdrawals?\s+and)", re.IGNORECASE)

# ── Groq LLM fallback ─────────────────────────────────────────────────────────
GROQ_URL   = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama-3.3-70b-versatile"
LLM_SYSTEM = """\
You are a financial statement parser. Extract ALL transactions from the bank or credit card statement text.

Return ONLY a valid JSON array. No markdown, no explanation — just the raw JSON array.

Each object must have exactly:
  "date"   : string YYYY-MM-DD
  "payee"  : string, clean merchant/description name
  "amount" : number, POSITIVE for purchases/charges, NEGATIVE for payments/credits/refunds/deposits

Rules:
- Include EVERY transaction, never skip
- Infer year from statement period if rows only show MM/DD
- Skip totals, balances, summaries, account headers, reward summaries
- Payments back to card (AUTOPAY, PAYMENT THANK YOU) are NEGATIVE
- Regular purchases are POSITIVE
- Return [] if no transactions found"""


@dataclass
class ParseResult:
    df: Optional[pd.DataFrame]
    confidence: float
    method: str
    warnings: list[str] = field(default_factory=list)
    source_label: str = "Universal"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s).strip().lower())

def _parse_date(val: str, statement_year: Optional[int] = None) -> Optional[str]:
    val = str(val).strip().rstrip("*").strip()
    # MM/DD without year — use statement_year or current year
    if re.match(r"^\d{1,2}/\d{1,2}$", val):
        year = statement_year or datetime.today().year
        val  = f"{val}/{year}"
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

def _apply_sign(amount: float, sign_convention: str) -> float:
    """Ensure positive = expense. Flip if bank exports expenses as negative."""
    if sign_convention == "inverted":
        return -amount
    return amount

def _clean_payee(p: str) -> str:
    p = re.sub(r'\s+\d+%\s+\$[\d.]+\s*$', '', p)   # Apple cashback
    p = re.sub(r'[\*\|]+$', '', p)
    return p.strip()

def _build_df(rows, filepath, source, confidence, method, warnings):
    if not rows:
        return ParseResult(None, 0.0, method, warnings, source)
    df = pd.DataFrame(rows)
    df["Source"]   = source
    df["Filepath"] = filepath
    return ParseResult(df, confidence, method, warnings, source)

def _detect_auto_sign(df: pd.DataFrame, col: str) -> str:
    """Auto-detect sign convention when no schema is known."""
    vals = pd.to_numeric(df[col], errors="coerce").dropna()
    if len(vals) == 0:
        return "normal"
    neg_pct = (vals < 0).sum() / len(vals)
    return "inverted" if neg_pct > 0.6 else "normal"

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

def _get_groq_key() -> Optional[str]:
    try:
        here = Path(__file__).resolve()
        for parent in [here.parent, here.parent.parent, here.parent.parent.parent]:
            candidate = parent / "settings.json"
            if candidate.exists():
                key = json.loads(candidate.read_text(encoding="utf-8")).get(
                    "groq_api_key", "").strip()
                if key and len(key) > 20:
                    return key
    except Exception:
        pass
    return os.environ.get("GROQ_API_KEY", "").strip() or None

def _infer_statement_year(text: str) -> Optional[int]:
    """Try to find a 4-digit year in the statement header/period line."""
    m = re.search(r"(20\d{2})", text[:500])
    return int(m.group(1)) if m else None


# ── Groq LLM ─────────────────────────────────────────────────────────────────

def _llm_parse(text: str, source: str, api_key: str) -> Optional[ParseResult]:
    MAX = 28000
    chunks = []
    if len(text) <= MAX:
        chunks = [text]
    else:
        pages = text.split("\f")
        cur = ""
        for p in pages:
            if len(cur) + len(p) > MAX:
                if cur: chunks.append(cur)
                cur = p
            else:
                cur += "\n" + p
        if cur: chunks.append(cur)

    all_rows = []
    for i, chunk in enumerate(chunks):
        payload = json.dumps({
            "model": GROQ_MODEL, "max_tokens": 4096, "temperature": 0,
            "messages": [
                {"role": "system", "content": LLM_SYSTEM},
                {"role": "user",   "content": f"Parse all transactions:\n\n{chunk}"}
            ]
        }).encode("utf-8")
        req = urllib.request.Request(
            GROQ_URL, data=payload,
            headers={"Content-Type": "application/json",
                     "Authorization": f"Bearer {api_key}"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = json.loads(resp.read())["choices"][0]["message"]["content"].strip()
            raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw)
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                all_rows.extend(parsed)
        except Exception as e:
            log.warning(f"[LLM] {source} chunk {i+1}: {e}")
            return None

    rows = []
    for r in all_rows:
        try:
            date   = _parse_date(str(r.get("date", "")))
            payee  = str(r.get("payee", "")).strip()
            amount = float(r.get("amount", 0))
            if date and payee and amount != 0:
                rows.append({"Date": date, "Payee": payee, "Amount": amount})
        except Exception:
            continue

    if not rows:
        return None
    log.info(f"[LLM] {source}: {len(rows)} transactions via Groq")
    df = pd.DataFrame(rows)
    df["Source"]   = source
    df["Filepath"] = ""
    return ParseResult(df, CONF_HIGH, "llm-groq", [], source)


# ── CSV Parser ────────────────────────────────────────────────────────────────

def _match_schema(headers: list[str], exclude_investment: bool = True) -> Optional[dict]:
    """
    Match headers against all known schemas.
    Uses match_any (list of candidate header sets) when present, else match[].
    Returns best match or None. Prefer longer/more-specific matches.
    """
    norm_hdrs = {_norm(h) for h in headers}
    best_schema = None
    best_name   = None
    best_score  = 0
    best_specificity = 0

    for name, schema in BANK_CSV_SCHEMAS.items():
        if schema.get("positional"):
            continue
        if exclude_investment and schema.get("investment_only"):
            continue

        # Build list of candidate match sets to try
        candidates = schema.get("match_any") or ([schema["match"]] if schema.get("match") else [])

        for candidate in candidates:
            if not candidate:
                continue
            required = {_norm(col) for col in candidate}
            matched  = required & norm_hdrs
            score    = len(matched)
            threshold = max(1, int(len(required) * 0.75))

            if score >= threshold and score > best_score:
                best_score       = score
                best_specificity = len(required)
                best_schema      = schema
                best_name        = name
            elif score == best_score and len(required) > best_specificity:
                # Prefer more specific schema when scores tie
                best_specificity = len(required)
                best_schema      = schema
                best_name        = name

    if best_name:
        log.debug(f"[Schema] matched '{best_name}' (score={best_score})")
    return best_schema


def _resolve_col(df_cols: list[str], primary: Optional[str],
                 fallbacks: list[str] = None) -> Optional[str]:
    """Find a column by primary name then fallbacks, using fuzzy norm matching."""
    candidates = ([primary] if primary else []) + (fallbacks or [])
    norm_map = {_norm(h): h for h in df_cols}
    for col in candidates:
        if col is None: continue
        # Exact norm match
        if _norm(col) in norm_map:
            return norm_map[_norm(col)]
        # Substring match
        for h_norm, h_orig in norm_map.items():
            if _norm(col) in h_norm or h_norm in _norm(col):
                return h_orig
    return None

def _parse_csv(path: Path) -> ParseResult:
    warnings = []
    source   = path.stem

    try:
        raw = path.read_text(encoding="utf-8-sig", errors="replace").splitlines()
    except Exception as e:
        return ParseResult(None, 0.0, "csv-read", [str(e)], source)

    # Find header row — skip legal disclaimers (Fidelity prepends 4 rows)
    header_idx = 0
    for i, line in enumerate(raw[:30]):
        parts = [p.strip().strip('"') for p in line.split(",")]
        non_num = sum(1 for p in parts if p and not re.match(r"^[\d\-\./\$\s]+$", p))
        if non_num >= 2 and len(parts) >= 2:
            header_idx = i
            break

    try:
        df_raw = pd.read_csv(path, skiprows=header_idx, encoding="utf-8-sig",
                             on_bad_lines="skip", dtype=str, thousands=",",
                             keep_default_na=False, na_filter=False)
    except Exception as e:
        return ParseResult(None, 0.0, "csv-table", [str(e)], source)

    df_raw.columns = df_raw.columns.str.strip().str.replace('"', '')
    headers = list(df_raw.columns)

    # Try to match a known bank schema
    schema = _match_schema(headers)

    # Wells Fargo has no headers — detect by structure
    if not schema and len(headers) in (4, 5) and not any(
            _norm(h) in [a for al in [DATE_ALIASES, PAYEE_ALIASES] for a in al]
            for h in headers):
        # Likely Wells Fargo: Date, Description, Credits, Debits, Balance
        schema = {"positional": True, "sign": "normal"}

    statement_year = None
    rows = []

    if schema and not schema.get("positional"):
        # ── Known schema — resolve columns with full fallback chains ───────────
        sign = schema.get("sign", "normal")

        date_col   = _resolve_col(headers, schema.get("date"),   schema.get("date_fallbacks", []))
        payee_col  = _resolve_col(headers, schema.get("payee"),  schema.get("payee_fallbacks", []))
        amount_col = _resolve_col(headers, schema.get("amount"), schema.get("amount_fallbacks", []))
        debit_col  = _resolve_col(headers, schema.get("debit"),  schema.get("debit_fallbacks", []))
        credit_col = _resolve_col(headers, schema.get("credit"), schema.get("credit_fallbacks", []))

        # Last-resort payee: any description-like column
        if not payee_col:
            payee_col = _find_col(headers, ["description", "memo", "name",
                                            "narrative", "details"])

        if not date_col:
            schema = None  # cannot parse without a date

        if schema and date_col:
            # Apply account_filter (Fidelity: CHECKING only)
            account_filter = schema.get("account_filter")
            if account_filter:
                acct_col = _find_col(headers, ["account"])
                if acct_col:
                    before = len(df_raw)
                    df_raw = df_raw[
                        df_raw[acct_col].astype(str).str.strip().str.upper()
                        == account_filter.upper()
                    ]
                    log.info(f"[CSV] account_filter={account_filter}: {before}→{len(df_raw)} rows")

            # Mint: Transaction Type column drives sign
            type_col_name = schema.get("type_col")
            type_col = _resolve_col(headers, type_col_name, []) if type_col_name else None

            # PayPal / Venmo: status/currency filters + string amount format
            status_filter   = schema.get("status_filter")
            currency_filter = schema.get("currency_filter")
            strip_currency  = schema.get("strip_currency", False)
            status_col   = _find_col(headers, ["status"])   if status_filter   else None
            currency_col = _find_col(headers, ["currency"]) if currency_filter else None

            for _, row in df_raw.iterrows():
                # Status filter (PayPal: Completed only, Venmo: Complete only)
                if status_col and status_filter:
                    sv = str(row.get(status_col, "")).strip().lower()
                    if status_filter.lower() not in sv:
                        continue

                # Currency filter (PayPal: USD only)
                if currency_col and currency_filter:
                    cv = str(row.get(currency_col, "")).strip().upper()
                    if cv != currency_filter.upper():
                        continue

                date = _parse_date(str(row.get(date_col, "")), statement_year)
                if not date: continue

                # Payee: walk all fallback columns until non-empty
                payee = ""
                all_payee_cols = []
                if payee_col: all_payee_cols.append(payee_col)
                for fb in schema.get("payee_fallbacks", []):
                    c_resolved = _resolve_col(headers, fb, [])
                    if c_resolved and c_resolved not in all_payee_cols:
                        all_payee_cols.append(c_resolved)
                for col in all_payee_cols:
                    v = str(row.get(col, "")).strip()
                    if v and v.lower() not in ("nan", "", "none", "no description", "n/a"):
                        payee = v
                        break
                if not payee: continue

                # Amount: Venmo/PayPal use "- $5.00" strings; others numeric
                amount = None
                raw_amt = str(row.get(amount_col, "")).strip() if amount_col else ""

                if strip_currency and raw_amt:
                    is_neg = raw_amt.startswith("-") or raw_amt.startswith("\u2212")
                    cleaned = re.sub(r"[+\u2212$,\s]", "", raw_amt).lstrip("-")
                    try:
                        amount = _apply_sign(-float(cleaned) if is_neg else float(cleaned), sign)
                    except ValueError:
                        pass
                elif raw_amt and raw_amt not in ("nan", ""):
                    a = _parse_amount(raw_amt)
                    if a is not None:
                        if type_col:
                            tx_type = str(row.get(type_col, "")).strip().lower()
                            if "debit" in tx_type:
                                amount = abs(a)
                            elif "credit" in tx_type:
                                amount = -abs(a)
                            else:
                                amount = _apply_sign(a, sign)
                        else:
                            amount = _apply_sign(a, sign)

                # Split debit/credit columns
                if amount is None and (debit_col or credit_col):
                    d = _parse_amount(str(row.get(debit_col,  "") or "")) or 0.0
                    k = _parse_amount(str(row.get(credit_col, "") or "")) or 0.0
                    if d or k:
                        amount = d - k  # debit=positive(expense), credit=negative(income)

                if amount is None: continue
                rows.append({"Date": date, "Payee": _clean_payee(payee), "Amount": amount})
            confidence = CONF_HIGH

    if schema and schema.get("positional"):
        # ── Wells Fargo positional (no headers) — use positional_map ──────────
        positional_map = schema.get("positional_map", {})
        for _, row in df_raw.iterrows():
            vals = [str(v).strip() for v in row.values]
            if len(vals) < 3: continue
            n = len(vals)
            mapping = positional_map.get(n) or positional_map.get(max(positional_map.keys(), default=5))
            if mapping:
                d_idx = mapping.get("date",   0)
                p_idx = mapping.get("payee",  3)
                a_idx = mapping.get("amount", 1)
                date   = _parse_date(vals[d_idx])
                payee  = _clean_payee(vals[p_idx])
                amount = _parse_amount(vals[a_idx])
            else:
                # Fallback heuristic
                date   = _parse_date(vals[0])
                amount = _parse_amount(vals[1])
                payee  = _clean_payee(vals[min(3, n-1)])
            if not date or amount is None: continue
            amount = _apply_sign(amount, schema.get("sign", "inverted"))
            rows.append({"Date": date, "Payee": payee, "Amount": amount})
        confidence = CONF_MEDIUM

    if not schema:
        # ── Generic column sniff ──────────────────────────────────────────────
        date_col   = _find_col(headers, DATE_ALIASES)
        payee_col  = _find_col(headers, PAYEE_ALIASES)
        amount_col = _find_col(headers, AMOUNT_ALIASES)
        debit_col  = _find_col(headers, DEBIT_ALIASES)
        credit_col = _find_col(headers, CREDIT_ALIASES)

        if not date_col:
            return ParseResult(None, 0.0, "csv-table", ["No date column"], source)
        if not payee_col:
            return ParseResult(None, 0.0, "csv-table", ["No payee column"], source)
        if not amount_col and not debit_col and not credit_col:
            return ParseResult(None, 0.0, "csv-table", ["No amount column"], source)

        sign = "normal"
        if amount_col:
            sign = _detect_auto_sign(
                df_raw.assign(**{amount_col: pd.to_numeric(df_raw[amount_col], errors="coerce")}),
                amount_col)

        cols_found = sum(x is not None for x in [date_col, payee_col,
                                                  amount_col or debit_col or credit_col])
        confidence = {3: CONF_HIGH, 2: CONF_MEDIUM}.get(cols_found, CONF_LOW)

        for _, row in df_raw.iterrows():
            date = _parse_date(str(row.get(date_col, "")), statement_year)
            if not date: continue
            payee = str(row.get(payee_col, "")).strip()
            if not payee or payee.lower() in ("nan", "", "none"): continue
            amount = None
            if amount_col and pd.notna(row.get(amount_col)):
                a = _parse_amount(row[amount_col])
                if a is not None:
                    amount = _apply_sign(a, sign)
            elif debit_col or credit_col:
                d = _parse_amount(row.get(debit_col, "") or "") or 0.0
                c = _parse_amount(row.get(credit_col, "") or "") or 0.0
                if d or c:
                    amount = d - c
            if amount is None: continue
            rows.append({"Date": date, "Payee": _clean_payee(payee), "Amount": amount})

    if not rows:
        # Last resort: try Groq
        api_key = _get_groq_key()
        if api_key:
            log.info(f"[CSV→LLM] {source}: rule-based got 0 rows, trying Groq")
            try:
                text = path.read_text(encoding="utf-8-sig", errors="replace")
                result = _llm_parse(text, source, api_key)
                if result:
                    result.df["Filepath"] = str(path)
                    return result
            except Exception as e:
                log.warning(f"[CSV→LLM] {source}: {e}")
        return ParseResult(None, 0.0, "csv-table", ["No rows extracted"], source)

    return _build_df(rows, str(path), source, confidence, "csv-schema" if schema else "csv-generic", warnings)


# ── PDF Parser ────────────────────────────────────────────────────────────────

def _extract_pdf_text(path: Path) -> str:
    parts = []
    try:
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                t = page.extract_text() or ""
                if t.strip(): parts.append(t)
    except Exception as e:
        log.warning(f"[PDF-text] {path.name}: {e}")
    return "\f".join(parts)

def _parse_pdf(path: Path) -> ParseResult:
    warnings = []
    source   = path.stem

    raw_text = _extract_pdf_text(path)
    stmt_year = _infer_statement_year(raw_text)

    # ── Strategy 1: pdfplumber tables ────────────────────────────────────────
    table_rows = []
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
                        if not table or len(table) < 2: continue
                        hdr_idx = 0
                        for ri, row in enumerate(table[:6]):
                            if row and sum(1 for c in row if c and str(c).strip()) >= 2:
                                hdr_idx = ri; break
                        raw_hdrs = [str(c or "").strip() for c in table[hdr_idx]]
                        schema = _match_schema(raw_hdrs)
                        if schema and not schema.get("positional"):
                            dc  = schema.get("date")
                            pc  = schema.get("payee")
                            ac  = schema.get("amount")
                            dbc = schema.get("debit")
                            crc = schema.get("credit")
                            sgn = schema.get("sign", "normal")
                        else:
                            dc  = _find_col(raw_hdrs, DATE_ALIASES)
                            pc  = _find_col(raw_hdrs, PAYEE_ALIASES)
                            ac  = _find_col(raw_hdrs, AMOUNT_ALIASES)
                            dbc = _find_col(raw_hdrs, DEBIT_ALIASES)
                            crc = _find_col(raw_hdrs, CREDIT_ALIASES)
                            sgn = "normal"
                        if not (dc and pc): continue
                        col_idx = {h: i for i, h in enumerate(raw_hdrs)}
                        def get(col, row):
                            idx = col_idx.get(col)
                            return str(row[idx]).strip() if idx is not None and idx < len(row) else ""
                        for row in table[hdr_idx + 1:]:
                            if not row or all(not str(c or "").strip() for c in row): continue
                            date  = _parse_date(get(dc, row), stmt_year)
                            if not date: continue
                            payee = _clean_payee(get(pc, row))
                            if not payee or payee.lower() in ("nan", "none", ""): continue
                            amount = None
                            if ac:
                                a = _parse_amount(get(ac, row))
                                if a is not None: amount = _apply_sign(a, sgn)
                            if amount is None and (dbc or crc):
                                d = _parse_amount(get(dbc, row) or "0") or 0.0
                                c = _parse_amount(get(crc, row) or "0") or 0.0
                                if d or c: amount = d - c
                            if amount is None: continue
                            page_rows.append({"Date": date, "Payee": payee, "Amount": amount})
                    if page_rows:
                        table_rows.extend(page_rows); break
    except Exception as e:
        warnings.append(f"Table error: {e}")

    # Trust tables unless Apple/Amex (where tables only get summary rows)
    min_trust = 10 if any(k in source.lower()
                          for k in ("apple", "amex", "american express")) else 3
    if len(table_rows) >= min_trust:
        return _build_df(table_rows, str(path), source, CONF_HIGH, "pdf-table", warnings)

    # ── Strategy 2: text line regex ───────────────────────────────────────────
    text_rows = []
    section_sign = None  # tracks deposit/withdrawal section for sectioned PDFs
    for line in raw_text.replace("\f", "\n").splitlines():
        line = line.strip()
        if not line or len(line) < 8 or SKIP_RE.match(line): continue

        # Detect section headers for WF/BofA/PNC style PDFs
        if DEPOSIT_SECTION_RE.match(line):
            section_sign = "deposit"
            continue
        if WITHDRAWAL_SECTION_RE.match(line):
            section_sign = "withdrawal"
            continue

        for pattern in PDF_PATTERNS:
            m = pattern.search(line)
            if not m: continue
            date   = _parse_date(m.group("date"), stmt_year)
            payee  = _clean_payee(m.group("payee"))
            amount = _parse_amount(m.group("amount"))
            if date and payee and amount is not None and len(payee) > 1:
                # Apply section context sign
                if section_sign == "deposit":
                    amount = -abs(amount)   # deposits are credits (negative = income)
                elif section_sign == "withdrawal":
                    amount = abs(amount)    # withdrawals are expenses (positive)
                text_rows.append({"Date": date, "Payee": payee, "Amount": amount})
                break

    # Merge, dedup by date+|amount|
    seen, combined = set(), []
    for r in text_rows + table_rows:
        key = (r["Date"], round(abs(r["Amount"]), 2))
        if key not in seen:
            seen.add(key); combined.append(r)

    if combined:
        method = ("pdf-table+text" if table_rows and text_rows else
                  "pdf-table"      if table_rows else "pdf-text")
        conf   = CONF_MEDIUM if len(combined) > 5 else CONF_LOW
        return _build_df(combined, str(path), source, conf, method, warnings)

    # ── Strategy 3: Groq LLM last resort ────────────────────────────────────
    api_key = _get_groq_key()
    if api_key and raw_text.strip():
        log.info(f"[PDF→LLM] {source}: rule-based got 0 rows, trying Groq")
        result = _llm_parse(raw_text, source, api_key)
        if result:
            result.df["Filepath"] = str(path)
            return result

    warnings.append("Zero rows extracted — may be scanned/image PDF")
    return ParseResult(None, 0.0, "pdf-none", warnings, source)


# ── Public API ────────────────────────────────────────────────────────────────

def parse_file(path: Path) -> ParseResult:
    ext = path.suffix.lower()
    if ext == ".csv":  return _parse_csv(path)
    if ext == ".pdf":  return _parse_pdf(path)
    return ParseResult(None, 0.0, "unsupported", [f"Unsupported: {ext}"], path.stem)

def parse_directory(directory: Path, min_confidence: float = CONF_LOW) -> pd.DataFrame:
    frames = []
    files  = sorted([f for f in directory.iterdir()
                     if f.suffix.lower() in (".csv", ".pdf")
                     and not f.name.startswith(".")])
    if not files:
        log.info(f"[StatementsParser] No files in {directory}")
        return pd.DataFrame(columns=["Date","Payee","Amount","Source","Filepath"])

    api_key = _get_groq_key()
    log.info(f"[StatementsParser] {len(files)} files | "
             f"LLM={'Groq' if api_key else 'off (no key)'}")

    for f in files:
        result = parse_file(f)
        for w in (result.warnings or []):
            log.debug(f"  [{result.method}] {f.name}: {w}")
        if result.df is None or result.df.empty:
            log.warning(f"  [{result.method}] {f.name}: 0 rows")
            continue
        if result.confidence < min_confidence:
            log.warning(f"  [{result.method}] {f.name}: "
                        f"confidence {result.confidence:.0%} < threshold")
            continue
        lbl = ("HIGH" if result.confidence >= CONF_HIGH else
               "MEDIUM" if result.confidence >= CONF_MEDIUM else "LOW")
        log.info(f"  [{result.method}] {f.name}: "
                 f"{len(result.df)} rows  {lbl} ({result.confidence:.0%})")
        frames.append(result.df)

    if not frames:
        return pd.DataFrame(columns=["Date","Payee","Amount","Source","Filepath"])
    return pd.concat(frames, ignore_index=True)
