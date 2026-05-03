"""
build_defaults.py
=================
Generates clean default JSON config files for distribution.
Called by the GitHub Actions build workflow.

Usage:  python build_defaults.py <output_dir>

Writes settings.json, savings_plan.json, and overrides.json
with no personal data — just sensible structural defaults
so the app starts up cleanly for a new user.
"""

import json
import sys
from pathlib import Path

dest = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".")
dest.mkdir(parents=True, exist_ok=True)

# ── settings.json — all zeros / empty, no personal data ─────────────────────
settings = {
    "names": {
        "p1": "Person 1",
        "p2": "Person 2"
    },
    "custom_cats": [],
    "income": {
        "salary":             0,
        "bonus":              0,
        "personal_401k_pct":  0.06,
        "company_401k_pct":   0.04,
        "tax_rate":           0.22,
        "insurance_yearly":   0,
        "rent_income":        0,
        "dividends":          0,
        "casie_salary":       0,
        "casie_personal_401k": 0.06,
        "casie_company_401k":  0.04,
        "casie_tax_rate":     0.22,
        "casie_insurance":    0,
        "casie_bonus":        0,
        "casie_rent_income":  0,
        "casie_dividends":    0,
    },
    "limits": {
        "Mortgage":     2000,
        "HOA":           300,
        "Grocery":       500,
        "Restaurant":    400,
        "Electric":      150,
        "Natural Gas":    75,
        "Internet":       80,
        "Car Payment":   400,
        "Car Services":  100,
        "Gym":            60,
        "MBTA":           20,
        "Amazon":        150,
        "Uber":           50,
        "Subscription":   50,
        "Veterinary":     75,
        "Dentist Office": 50,
        "Insurance":     250,
        "All-Other":     500,
    }
}

# ── savings_plan.json — structural defaults, no personal balances ─────────────
from datetime import datetime, timedelta
today = datetime.today()
start_month = today.strftime("%Y-%m")
ready_by    = (today.replace(year=today.year + 3)).strftime("%Y-%m")

savings_plan = {
    "plan_type":               "sell_buy",
    "sean_interest":           0.08,
    "casie_interest":          0.08,
    "ready_by":                ready_by,
    "target_price":            500000,
    "down_pct":                0.20,
    "mortgage_rate":           0.07,
    "mortgage_term":           360,
    "condo_value":             0,
    "condo_rate":              0.003,
    "condo_mortgage_rate":     0.04,
    "condo_principle":         0,
    "condo_purchase_price":    0,
    "condo_monthly_principal": 0,
    "sean_start_bal":          0,
    "casie_start_bal":         0,
    "start_month":             start_month,
    "sean_default_monthly":    1000,
    "casie_default_monthly":   1000,
    "sean_default_income":     5000,
    "casie_default_income":    4000,
    "year_incomes":            {},
    "months":                  {},
    "liq_dp_pct":              80,
    "liq_res_pct":             20,
}

# ── overrides.json — empty ────────────────────────────────────────────────────
overrides = {}

# ── Write files ───────────────────────────────────────────────────────────────
(dest / "settings.json").write_text(json.dumps(settings, indent=2))
(dest / "savings_plan.json").write_text(json.dumps(savings_plan, indent=2))
(dest / "overrides.json").write_text(json.dumps(overrides, indent=2))

print(f"OK: Wrote clean defaults to {dest}/")
print(f"  settings.json      - no personal income data")
print(f"  savings_plan.json  - ready_by set to {ready_by}, all balances = 0")
print(f"  overrides.json     - empty")
