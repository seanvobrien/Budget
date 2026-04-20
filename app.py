"""Financial Planner — app.py"""
import os, json, logging, subprocess, sys, threading, csv, re
from datetime import datetime, timedelta
from pathlib import Path
from flask import Flask, jsonify, request, Response, stream_with_context, send_file
from flask_cors import CORS

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
app = Flask(__name__)
CORS(app)

# ── Path setup ────────────────────────────────────────────────────────────────
if getattr(sys, 'frozen', False):
    BUNDLE_DIR = Path(sys._MEIPASS)
    BASE_DIR   = Path(sys.executable).parent
else:
    BUNDLE_DIR = Path(__file__).parent
    BASE_DIR   = Path(__file__).parent

# ── Load .env file if present (keeps API keys out of git) ────────────────────
_env_file = BASE_DIR / ".env"
if _env_file.exists():
    for _line in _env_file.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

OUTPUT_DIR     = BASE_DIR / "output"
PLAN_FILE      = BASE_DIR / "savings_plan.json"
OVERRIDES_FILE = BASE_DIR / "overrides.json"
SETTINGS_FILE  = BASE_DIR / "settings.json"

_sse_clients, _sse_lock = [], threading.Lock()
state_lock = threading.Lock()

def push_event(ev, data):
    msg = f"event: {ev}\ndata: {json.dumps(data)}\n\n"
    with _sse_lock:
        for q in list(_sse_clients):
            try: q.put_nowait(msg)
            except: pass

def add_log(msg, level="info"):
    log.info(msg)
    push_event("log", {"message": msg, "level": level, "ts": datetime.now().strftime("%H:%M:%S")})

def load_json(path, default):
    p = Path(path)
    if p.exists():
        try: return json.loads(p.read_text())
        except: pass
    return json.loads(json.dumps(default))

def save_json(path, data):
    Path(path).write_text(json.dumps(data, indent=2))

DEFAULT_SETTINGS = {
    "names": {"p1": "Person 1", "p2": "Person 2"},
    "custom_cats": [],
    "groq_api_key": "",
    "income": {
        "salary":              0,
        "bonus":               0,
        "personal_401k_pct":   0.06,
        "company_401k_pct":    0.04,
        "tax_rate":            0.22,
        "insurance_yearly":    0,
        "rent_income":         0,
        "dividends":           0,
        "casie_salary":        0,
        "casie_personal_401k": 0.06,
        "casie_company_401k":  0.04,
        "casie_tax_rate":      0.22,
        "casie_insurance":     0,
        "casie_bonus":         0,
        "casie_rent_income":   0,
        "casie_dividends":     0,
    },
    "limits": {
        "Mortgage":0,"HOA":0,"Grocery":0,"Restaurant":0,
        "Electric":0,"Natural Gas":0,"Internet":0,"Car Payment":0,
        "Car Services":0,"Gym":0,"MBTA":0,"Amazon":0,"Uber":0,
        "Subscription":0,"Veterinary":0,"Dentist Office":0,"Insurance":0,"All-Other":0,
    }
}

def _default_plan():
    from datetime import datetime
    today = datetime.today()
    start = today.strftime("%Y-%m")
    ready = today.replace(year=today.year + 3).strftime("%Y-%m")
    return {
        "plan_type":               "sell_buy",
        "sean_interest":           0.08,
        "casie_interest":          0.08,
        "ready_by":                ready,
        "target_price":            0,
        "down_pct":                0.20,
        "mortgage_rate":           0.07,
        "mortgage_term":           360,
        "condo_value":             0,
        "condo_rate":              0.003,
        "condo_mortgage_rate":     0.04,
        "condo_principle":         0,
        "condo_purchase_price":    0,
        "condo_monthly_principal": 0,
        "sean_start_bal":          0.0,
        "casie_start_bal":         0.0,
        "start_month":             start,
        "sean_default_monthly":    0,
        "casie_default_monthly":   0,
        "sean_default_income":     0,
        "casie_default_income":    0,
        "year_incomes":            {},
        "months":                  {},
        "liq_dp_pct":              80,
        "liq_res_pct":             20,
        "condo_purchase_date":     "",
        "condo_value_date":        "",
    }

DEFAULT_PLAN = _default_plan()

SETTINGS  = load_json(SETTINGS_FILE,  DEFAULT_SETTINGS)
PLAN      = load_json(PLAN_FILE,      DEFAULT_PLAN)
OVERRIDES = load_json(OVERRIDES_FILE, {})


def _create_shortcut_once():
    """
    Silently create a desktop shortcut the first time the app launches.
    Only runs on Windows. Writes .shortcut_created flag so it never runs again.
    """
    flag = BASE_DIR / ".shortcut_created"
    if flag.exists() or sys.platform != "win32":
        return
    try:
        import subprocess, os
        exe_path     = BASE_DIR / "Budget.exe"
        ico_path     = BASE_DIR / "Budget.ico"
        desktop_path = Path(os.path.expanduser("~")) / "Desktop" / "Budget.lnk"

        # Only create shortcut if the exe actually exists (i.e. running as bundle)
        if not exe_path.exists():
            return

        vbs = f'''Set oWS = WScript.CreateObject("WScript.Shell")
Set oLink = oWS.CreateShortcut("{desktop_path}")
oLink.TargetPath = "{exe_path}"
oLink.WorkingDirectory = "{BASE_DIR}"
oLink.Description = "Budget — Personal Finance Dashboard"
oLink.WindowStyle = 1
'''
        if ico_path.exists():
            vbs += f'oLink.IconLocation = "{ico_path}"\n'
        vbs += "oLink.Save\n"

        vbs_path = BASE_DIR / "_tmp_sc.vbs"
        vbs_path.write_text(vbs)
        subprocess.run(["cscript", "//nologo", str(vbs_path)],
                       capture_output=True, timeout=10)
        vbs_path.unlink(missing_ok=True)
        flag.touch()   # mark done — never runs again
        log.info("Desktop shortcut created.")
    except Exception as e:
        log.warning(f"Shortcut creation skipped: {e}")




# ── Statement folder helpers ─────────────────────────────────────────────────

def _stmts_root() -> Path:
    for name in ("Statements", "statements"):
        p = BASE_DIR / name
        if p.exists():
            return p
    return BASE_DIR / "Statements"

def _stmts_path(sub: str) -> Path:
    return _stmts_root() / sub

EXPENSE_CATS = {
    "Grocery","Restaurant","Electric","Natural Gas","Internet","Car Services",
    "Car Payment","Gym","MBTA","Amazon","Uber","Subscription","Veterinary",
    "Dentist Office","Insurance","HOA","Mortgage"
}

def derived_income(s):
    inc  = s["income"]
    sal  = inc.get("salary", 0)
    tx   = inc.get("tax_rate", 0.226)
    ins  = inc.get("insurance_yearly", 0)
    p401 = inc.get("personal_401k_pct", 0.06)
    c401 = inc.get("company_401k_pct",  0.04)
    bonus = inc.get("bonus", 0)
    rent  = inc.get("rent_income", 0)
    divs  = inc.get("dividends", 0)
    taxes    = round(sal * tx, 2)
    ret_p    = round(sal * p401, 2)
    ret_c    = round(sal * c401, 2)
    net_sal_y = sal - taxes - ins - ret_p
    net_monthly = round(net_sal_y / 12, 2)
    rent_mo  = round(rent / 12, 2)
    divs_mo  = round(divs / 12, 2)
    net_monthly_total = round(net_monthly + rent_mo + divs_mo, 2)
    net_yearly = round(net_sal_y + bonus + rent + divs, 2)

    # Person 2 — full matching fields
    c_sal   = inc.get("casie_salary", 0)
    c_tx    = inc.get("casie_tax_rate", 0.20)
    c_ins   = inc.get("casie_insurance", 0)
    c_p401  = inc.get("casie_personal_401k", p401)   # falls back to P1 rate if not set
    c_c401  = inc.get("casie_company_401k",  c401)
    c_bonus = inc.get("casie_bonus", 0)
    c_rent  = inc.get("casie_rent_income", 0)
    c_divs  = inc.get("casie_dividends", 0)
    c_taxes  = round(c_sal * c_tx, 2)
    c_ret_p  = round(c_sal * c_p401, 2)
    c_ret_c  = round(c_sal * c_c401, 2)
    c_net_sal_y = c_sal - c_taxes - c_ins - c_ret_p
    c_net_m  = round(c_net_sal_y / 12 + c_rent / 12 + c_divs / 12, 2)

    return {
        **inc,
        "taxes_yearly":        taxes,
        "retirement_personal": ret_p,
        "retirement_company":  ret_c,
        "gross_monthly":       round(sal/12, 2),
        "net_monthly":         net_monthly_total,
        "net_yearly":          net_yearly,
        "rent_monthly":        rent_mo,
        "dividends_monthly":   divs_mo,
        "bonus_note":          "Lump sum — excluded from monthly operating income",
        "casie_taxes_yearly":  c_taxes,
        "casie_retirement_personal": c_ret_p,
        "casie_retirement_company":  c_ret_c,
        "casie_net_monthly":   c_net_m,
        "combined_net_monthly": net_monthly_total + c_net_m,
    }

def run_parser():
    import tempfile, shutil, contextlib, importlib
    add_log("Running statement parsers...", "info")
    parse_results = []

    # OneDrive / iCloud / Dropbox: copy to local temp before parsing
    stmts_root = None
    for name in ("statements", "Statements"):
        p = BASE_DIR / name
        if p.exists():
            stmts_root = p
            break
    use_base = BASE_DIR
    tmp_dir  = None
    if stmts_root:
        cloud_kw = ("onedrive", "icloud", "dropbox", "google drive", "box")
        if any(kw in str(stmts_root).lower() for kw in cloud_kw):
            add_log("Cloud folder detected - copying to temp for parsing...", "info")
            tmp_dir   = Path(tempfile.mkdtemp(prefix="budget_parse_"))
            tmp_stmts = tmp_dir / "statements"
            shutil.copytree(str(stmts_root), str(tmp_stmts))
            use_base  = tmp_dir
            add_log(f"Temp copy ready at {tmp_dir}", "info")

    try:
        if getattr(sys, "frozen", False):
            if str(BUNDLE_DIR) not in sys.path:
                sys.path.insert(0, str(BUNDLE_DIR))
            run_mod = importlib.import_module("run")
            importlib.reload(run_mod)
            class _Capture:
                def write(self, s):
                    if s.strip(): add_log(s.strip(), "info")
                def flush(self): pass
            with contextlib.redirect_stdout(_Capture()):
                run_mod.main(use_base)
            add_log("Statements parsed successfully.", "success")
            return {"ok": True, "parse_results": []}
        else:
            r = subprocess.run(
                [sys.executable, str(BUNDLE_DIR / "run.py")],
                cwd=str(use_base), capture_output=True, text=True,
                timeout=180, encoding="utf-8", errors="replace"
            )
            for line in (r.stdout or "").strip().splitlines():
                if not line.strip(): continue
                add_log(line.strip(), "info")
                m = re.match(
                    r"\[(?:statements_parser|StatementsParser):(?P<method>[\w-]+)\]\s+"
                    r"(?P<file>.+?):\s+(?P<rows>\d+) rows,\s+confidence=(?P<label>\w+)"
                    r"\s+\((?P<pct>[\d.]+)%\)", line.strip()
                )
                if m:
                    parse_results.append({
                        "file": m.group("file"), "method": m.group("method"),
                        "rows": int(m.group("rows")),
                        "confidence": float(m.group("pct")) / 100, "warnings": [],
                    })
            if r.returncode == 0:
                add_log("Statements parsed successfully.", "success")
                return {"ok": True, "parse_results": parse_results}
            err = (r.stderr or "unknown error")[:400]
            add_log(f"Parser error: {err}", "error")
            return {"ok": False, "error": err, "parse_results": parse_results}
    except subprocess.TimeoutExpired:
        add_log("Parser timed out", "error")
        return {"ok": False, "error": "Timeout", "parse_results": []}
    except Exception as e:
        add_log(str(e), "error")
        return {"ok": False, "error": str(e), "parse_results": []}
    finally:
        if tmp_dir and tmp_dir.exists():
            # Copy parsed output back to the real output folder before cleanup
            tmp_output = tmp_dir / "output"
            if tmp_output.exists():
                OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
                import shutil as _sh
                for f in tmp_output.iterdir():
                    _sh.copy2(str(f), str(OUTPUT_DIR / f.name))
                add_log(f"Output copied to {OUTPUT_DIR}", "info")
            shutil.rmtree(str(tmp_dir), ignore_errors=True)

def load_transactions():
    p = OUTPUT_DIR / "transactions.csv"
    if not p.exists(): return []
    rows = []
    with open(p, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            try:
                key = f"{r['Date']}|{r['Payee']}|{r['Amount']}"
                ov  = OVERRIDES.get(key, {})
                cat = ov.get("category", r["Category"])
                exc = ov.get("exclude", False) or cat == "Exclude"
                rows.append({
                    "date": r["Date"], "payee": r["Payee"],
                    "amount": float(r["Amount"]), "source": r.get("Source",""),
                    "year": int(r["Year"]), "month": int(r["Month"]),
                    "month_val": r["Month_Val"], "flow": r["Flow"],
                    "category": cat, "original_category": r["Category"],
                    "key": f"{int(r['Year'])}-{int(r['Month']):02d}",
                    "tx_key": key, "excluded": exc, "overridden": bool(ov),
                })
            except: pass
    return rows

def load_investments():
    p = OUTPUT_DIR / "portfolio_positions.csv"
    if not p.exists(): return [], None
    rows, snap = [], None
    with open(p, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            try:
                parts = r["Account_Symbol"].split("_")
                rows.append({
                    "symbol": "_".join(parts[1:]), "account": parts[0],
                    "quantity": float(r["Quantity"]), "avg_cost": float(r["Average_Cost_Basis"]),
                })
                if not snap:
                    m = re.search(r"(\w{3}-\d{2}-\d{4})", r.get("Filepath",""))
                    if m: snap = m.group(1)
            except: pass
    return rows, snap

def _is_month_complete(key: str, txns: list) -> bool:
    """
    Returns True if the month is complete enough to display.
    A month is considered complete if:
      - It is not the current calendar month, OR
      - It IS the current month AND we have credit statement data
        that covers at least through the 20th of the month
        (i.e. the latest credit transaction date is >= the 20th).
    This prevents a partial current month from skewing the dashboard.
    """
    today = datetime.today()
    current_key = today.strftime("%Y-%m")
    if key != current_key:
        return True  # Past months always shown
    # For the current month: check if the latest credit transaction
    # date reaches far enough into the month to be representative.
    # We require at least one credit/debit transaction dated on or after the 20th.
    month_txns = [t for t in txns if t["key"] == key and not t["excluded"]]
    if not month_txns:
        return False
    # Find the latest transaction date in this month
    dates = []
    for t in month_txns:
        try:
            dates.append(datetime.strptime(t["date"], "%Y-%m-%d"))
        except Exception:
            pass
    if not dates:
        return False
    latest_day = max(d.day for d in dates)
    # Only show current month if statements cover at least through the 20th
    return latest_day >= 20


def aggregate_monthly(txns):
    from collections import defaultdict
    from calendar import month_abbr as ma
    mo = defaultdict(lambda:{"income":0.0,"contributions":0.0,"categories":defaultdict(float)})
    for r in txns:
        if r["excluded"]: continue
        k, cat, amt = r["key"], r["category"], r["amount"]
        if cat == "Pay Check":     mo[k]["income"] += amt
        elif cat == "Contribution":
            if amt < 0:            mo[k]["contributions"] += abs(amt)
        elif cat in EXPENSE_CATS:  mo[k]["categories"][cat] += abs(amt)
    result = []
    for k in sorted(mo.keys()):
        if k == "2023-12": continue
        if not _is_month_complete(k, txns): continue  # Skip incomplete current month
        yr, m = k.split("-")
        d = mo[k]; exp = sum(d["categories"].values())
        result.append({
            "key": k, "label": f"{ma[int(m)]} '{yr[2:]}",
            "income": round(d["income"],2), "expenses": round(exp,2),
            "contributions": round(d["contributions"],2),
            "net": round(d["income"]-exp-d["contributions"],2),
            "categories": {c:round(v,2) for c,v in d["categories"].items()},
        })
    return result

def get_stock_prices(symbols):
    prices = {}
    try:
        import yfinance as yf
        unique = list(set(symbols))
        tickers = yf.Tickers(" ".join(unique))
        for sym in unique:
            try:
                info = tickers.tickers[sym].fast_info
                prices[sym] = round(float(info.last_price), 2)
            except:
                prices[sym] = None
    except ImportError:
        add_log("yfinance not installed — pip install yfinance", "error")
    except Exception as e:
        add_log(f"Price fetch error: {e}", "error")
    return prices

def _months_range(start, end_inclusive):
    result, cur = [], datetime.strptime(start, "%Y-%m")
    end = datetime.strptime(end_inclusive, "%Y-%m")
    while cur <= end:
        result.append(cur.strftime("%Y-%m"))
        cur = (cur.replace(day=1) + timedelta(days=32)).replace(day=1)
    return result

def compute_projection(plan):
    from calendar import month_abbr as ma
    months_data = plan.get("months", {})
    sean_rate  = plan.get("sean_interest", 0.08205) / 12
    casie_rate = plan.get("casie_interest", 0.08205) / 12
    sean_bal   = float(plan.get("sean_start_bal", 0))
    casie_bal  = float(plan.get("casie_start_bal", 0))
    start      = plan.get("start_month", "2025-06")
    sean_def   = float(plan.get("sean_default_monthly", 2000))
    casie_def  = float(plan.get("casie_default_monthly", 2000))
    tp         = float(plan.get("target_price", 0))
    dp_pct     = float(plan.get("down_pct", 0.20))
    mort_rate  = float(plan.get("mortgage_rate", 0.055))
    mort_term  = int(plan.get("mortgage_term", 360))
    condo_val  = float(plan.get("condo_value", 0))
    condo_rate = float(plan.get("condo_rate", 0.003))
    condo_purchase_price = float(plan.get("condo_purchase_price", 0))
    condo_principle_paid = float(plan.get("condo_principle", 0))
    _condo_monthly_principal = float(plan.get("condo_monthly_principal", 850))

    # Anchor condo_val AND condo_principle_paid to value_date, not start_month.
    # The loop pre-increments (condo_val *= rate, principal += monthly) BEFORE
    # appending each row. So the row labelled start_month already reflects one
    # month of growth past the init value. To land on condo_value exactly at
    # value_date we back-project (offset + 1) months, where offset = months
    # from start_month to value_date.
    _value_date_str = plan.get("condo_value_date", "")
    if _value_date_str and condo_val > 0:
        try:
            _vd = datetime.strptime(_value_date_str[:7], "%Y-%m")
            _sd = datetime.strptime(start, "%Y-%m")
            _months_offset = (_vd.year - _sd.year) * 12 + (_vd.month - _sd.month)
            _eff = _months_offset + 1
            if condo_rate > 0:
                condo_val = condo_val / ((1 + condo_rate) ** _eff)
            condo_principle_paid = max(
                condo_principle_paid - (_condo_monthly_principal * _eff), 0
            )
        except Exception:
            pass
    # If current home mortgage rate is provided, refine monthly principal estimate
    _condo_mort_rate = float(plan.get("condo_mortgage_rate", 0.0))
    if _condo_mort_rate > 0 and condo_purchase_price > 0:
        _orig_loan    = max(condo_purchase_price * 0.8, condo_purchase_price - 50000)
        _remaining    = max(_orig_loan - condo_principle_paid, 0)
        _mr_mo        = _condo_mort_rate / 12
        if _mr_mo > 0 and _remaining > 0:
            _total_pmt       = (_remaining * _mr_mo * (1+_mr_mo)**360) / ((1+_mr_mo)**360 - 1)
            _interest_mo     = _remaining * _mr_mo
            _derived_principal = max(_total_pmt - _interest_mo, 0)
            if _derived_principal > 0:
                _condo_monthly_principal = _derived_principal
    plan_type  = plan.get("plan_type", "sell_buy")
    use_property = (plan_type == "sell_buy")
    inc        = derived_income(SETTINGS)
    today      = datetime.today().replace(day=1)
    current    = datetime.strptime(start, "%Y-%m")
    # Project through ready_by + 18 months buffer, minimum through 2 years from start
    ready_by_str = plan.get("ready_by", "2028-06-01")
    try:
        ready_dt = datetime.strptime(ready_by_str[:7], "%Y-%m")
    except ValueError:
        ready_dt = datetime(2028, 6, 1)
    end_dt = max(
        ready_dt + timedelta(days=548),        # ready_by + ~18 months
        current + timedelta(days=730),          # at least 2 years from start
        datetime(current.year + 2, 12, 1),      # at least through 2 full years
    )
    total_principle_in = float(plan.get("sean_start_bal", 0)) + float(plan.get("casie_start_bal", 0))
    rows = []
    while current <= end_dt:
        key    = current.strftime("%Y-%m")
        mo     = months_data.get(key, {})
        is_fut = current > today
        yr_key = str(current.year)
        yr_inc = plan.get("year_incomes", {}).get(yr_key, {})
        s_inc_default = float(yr_inc.get("sean", plan.get("sean_default_income", 0)))
        c_inc_default = float(yr_inc.get("casie", plan.get("casie_default_income", 0)))
        s_inc = float(mo.get("sean_income", s_inc_default))
        c_inc = float(mo.get("casie_income", c_inc_default))
        for ov in plan.get("income_overrides", []):
            if not ov.get("from") or not ov.get("to"): continue
            if ov["from"] <= key <= ov["to"]:
                monthly = None
                if ov.get("monthly") is not None:   monthly = float(ov["monthly"])
                elif ov.get("yearly") is not None:  monthly = float(ov["yearly"]) / 12
                if monthly is not None:
                    if ov.get("person") == "sean":   s_inc = monthly
                    elif ov.get("person") == "casie": c_inc = monthly
        sc = float(mo.get("sean_contrib",  sean_def if is_fut else 0))
        cc = float(mo.get("casie_contrib", casie_def if is_fut else 0))
        total_principle_in += sc + cc
        sean_bal  = sean_bal  * (1 + sean_rate)  + sc
        casie_bal = casie_bal * (1 + casie_rate) + cc
        combined  = sean_bal + casie_bal
        s_equity  = max(combined - total_principle_in, 0)
        s_taxes   = s_equity * 0.15
        s_return  = combined - s_taxes

        # Property equity — only computed when plan_type = sell_buy
        if use_property:
            condo_val          *= (1 + condo_rate)
            condo_principle_paid += _condo_monthly_principal
            condo_equity         = condo_val - condo_purchase_price
            condo_combined       = condo_principle_paid + condo_equity
            condo_closing        = condo_val * 0.10
            condo_return         = max(condo_combined - condo_closing, 0)
        else:
            condo_equity   = 0.0
            condo_combined = 0.0
            condo_closing  = 0.0
            condo_return   = 0.0

        total_liq  = s_return + condo_return
        dp_needed  = tp * dp_pct + tp * 0.025 if plan_type != "savings" else tp
        dp_pct_ach = total_liq / tp if tp > 0 else 0
        if plan_type == "savings":
            # Savings-only: no mortgage/housing projection
            mort_pay    = 0.0
            tax_mo      = 0.0
            ins_mo      = 0.0
            tot_housing = 0.0
            housing_pct = 0.0
            op_budget   = s_inc + c_inc
        else:
            loan = max(tp - total_liq, 0)
            mr   = mort_rate / 12
            mort_pay = (loan*mr*(1+mr)**mort_term/((1+mr)**mort_term-1)) if loan>0 and mr>0 else 0
            tax_mo   = tp * 0.012 / 12
            ins_mo   = tp * 0.005 / 12
            tot_housing = mort_pay + tax_mo + ins_mo
            housing_pct = tot_housing / (s_inc+c_inc) if (s_inc+c_inc)>0 else 0
            op_budget   = (s_inc+c_inc) - tot_housing
        rows.append({
            "key": key, "label": f"{ma[current.month]} '{str(current.year)[2:]}",
            "year": current.year, "is_future": is_fut,
            "sean_income": round(s_inc,2), "casie_income": round(c_inc,2),
            "combined_income": round(s_inc+c_inc,2),
            "sean_income_yearly": round(s_inc * 12, 2),
            "casie_income_yearly": round(c_inc * 12, 2),
            "combined_income_yearly": round((s_inc + c_inc) * 12, 2),
            "sean_contrib": sc, "casie_contrib": cc,
            "total_contrib": sc+cc,
            "sean_bal": round(sean_bal,2), "casie_bal": round(casie_bal,2),
            "combined_bal": round(combined,2),
            "savings_principle": round(total_principle_in,2),
            "savings_equity": round(s_equity,2),
            "savings_taxes": round(s_taxes,2),
            "savings_return": round(s_return,2),
            "condo_value": round(condo_val,2),
            "condo_mortgage_rate": plan.get("condo_mortgage_rate", 0.03),
            "condo_principle_paid": round(condo_principle_paid,2),
            "condo_equity": round(condo_equity,2),
            "condo_combined": round(condo_combined,2),
            "condo_closing": round(condo_closing,2),
            "condo_return": round(condo_return,2),
            "total_liquidity": round(total_liq,2),
            "dp_needed": round(dp_needed,2),
            "dp_dollars": round(total_liq,2),
            "dp_pct": round(dp_pct_ach,4),
            "mortgage_payment": round(mort_pay,2),
            "tax_mo": round(tax_mo,2),
            "ins_mo": round(ins_mo,2),
            "total_housing": round(tot_housing,2),
            "housing_pct": round(housing_pct,4),
            "op_budget": round(op_budget,2),
        })
        current = (current.replace(day=1) + timedelta(days=32)).replace(day=1)
    return rows

def _append_category_rule(keyword: str, category: str):
    """Append a new (keyword, category) rule to src/mapping.py at runtime."""
    mapping_path = BUNDLE_DIR / "src" / "mapping.py"
    if not mapping_path.exists():
        log.warning(f"mapping.py not found at {mapping_path} — rule not persisted")
        return
    try:
        content = mapping_path.read_text(encoding="utf-8")
        # Find the last entry in MAPPING list and insert after it
        insert_line = f'    ("{keyword}", "{category}"),\n'
        # Find the closing bracket of MAPPING
        marker = "]\n\n# ── High-priority"
        if marker in content:
            content = content.replace(marker, insert_line + marker)
        else:
            # Fallback: append before the HIGH_PRIORITY block
            fallback = "\nHIGH_PRIORITY"
            content = content.replace(fallback, f"\n{insert_line}{fallback}")
        mapping_path.write_text(content, encoding="utf-8")
        log.info(f"Added mapping rule: '{keyword}' → '{category}'")
    except Exception as e:
        log.error(f"Failed to update mapping.py: {e}")

# ── ROUTES ───────────────────────────────────────────────────────────────────

@app.route("/")
def index(): return send_file(BUNDLE_DIR / "budget.html")

@app.route("/favicon.ico")
def favicon():
    ico = BUNDLE_DIR / "Budget.ico"
    if ico.exists():
        return send_file(ico, mimetype="image/x-icon")
    return "", 204

@app.route("/api/groq-key", methods=["GET","POST"])
def groq_key():
    global SETTINGS
    if request.method == "POST":
        key = (request.json or {}).get("key", "").strip()
        SETTINGS["groq_api_key"] = key
        save_json(SETTINGS_FILE, SETTINGS)
        return jsonify({"ok": True})
    return jsonify({"key": SETTINGS.get("groq_api_key", "")})

@app.route("/api/plan/calc-appreciation", methods=["POST"])
def calc_appreciation():
    """
    Given purchase_price, purchase_date, current_value, value_date,
    compute the implied monthly appreciation rate and return it.
    Also saves condo_purchase_date and condo_value_date to the plan.
    """
    data = request.get_json() or {}
    try:
        purchase_price  = float(data.get("purchase_price", 0))
        current_value   = float(data.get("current_value", 0))
        purchase_date   = data.get("purchase_date", "")   # YYYY-MM-DD or YYYY-MM
        value_date      = data.get("value_date", "")      # YYYY-MM-DD or YYYY-MM
        if not purchase_price or not current_value or not purchase_date or not value_date:
            return jsonify({"error": "Missing required fields"}), 400
        # Parse dates — accept YYYY-MM or YYYY-MM-DD
        def _parse_dt(s):
            for fmt in ("%Y-%m-%d", "%Y-%m"):
                try: return datetime.strptime(s[:10 if fmt=="%Y-%m-%d" else 7], fmt)
                except ValueError: pass
            raise ValueError(f"Cannot parse date: {s}")
        dt_purchase = _parse_dt(purchase_date)
        dt_value    = _parse_dt(value_date)
        months = (dt_value.year - dt_purchase.year) * 12 + (dt_value.month - dt_purchase.month)
        if months <= 0:
            return jsonify({"error": "Value date must be after purchase date"}), 400
        # (1 + r)^months = current_value / purchase_price
        ratio = current_value / purchase_price
        monthly_rate = ratio ** (1.0 / months) - 1.0
        # Save dates to plan
        with state_lock:
            PLAN["condo_purchase_date"] = purchase_date
            PLAN["condo_value_date"]    = value_date
            PLAN["condo_rate"]          = round(monthly_rate, 6)
        save_json(PLAN_FILE, PLAN)
        return jsonify({
            "ok": True,
            "monthly_rate": round(monthly_rate, 6),
            "monthly_rate_pct": round(monthly_rate * 100, 4),
            "months": months,
            "implied_annual_pct": round(((1 + monthly_rate) ** 12 - 1) * 100, 2),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/api/shutdown", methods=["POST"])
def shutdown():
    add_log("Server shutting down...", "info")
    def _exit():
        import time; time.sleep(0.3); os._exit(0)
    threading.Thread(target=_exit, daemon=True).start()
    return jsonify({"ok": True})

# ── Heartbeat — frontend pings every 5s; if no ping for 15s, shut down ────────
_last_ping = {"t": None}

@app.route("/api/ping", methods=["POST"])
def ping():
    import time
    _last_ping["t"] = time.time()
    return jsonify({"ok": True})

def _watchdog():
    """Shut down if the browser tab has been closed for 15 seconds."""
    import time
    time.sleep(10)   # grace period on startup
    _last_ping["t"] = time.time()
    while True:
        time.sleep(5)
        if _last_ping["t"] and (time.time() - _last_ping["t"]) > 15:
            add_log("Browser closed — shutting down.", "info")
            time.sleep(0.5)
            os._exit(0)

threading.Thread(target=_watchdog, daemon=True).start()

@app.route("/api/events")
def events():
    import queue
    q = queue.Queue(maxsize=50)
    with _sse_lock: _sse_clients.append(q)
    def gen():
        try:
            yield "data: connected\n\n"
            while True:
                try:    yield q.get(timeout=30)
                except: yield ": ping\n\n"
        except GeneratorExit:
            with _sse_lock:
                try: _sse_clients.remove(q)
                except: pass
    return Response(stream_with_context(gen()), mimetype="text/event-stream",
                    headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

@app.route("/api/parse", methods=["POST"])
def parse():
    r = run_parser()
    if r["ok"]: push_event("data_refreshed", {"ts": datetime.now().isoformat()})
    return jsonify(r)

@app.route("/api/data")
def get_data():
    txns = load_transactions()
    port, snap = load_investments()
    p = OUTPUT_DIR / "transactions.csv"
    return jsonify({
        "monthly": aggregate_monthly(txns),
        "portfolio": port,
        "snapshot_date": snap,
        "limits": SETTINGS.get("limits", DEFAULT_SETTINGS["limits"]),
        "income": derived_income(SETTINGS),
        "names": SETTINGS.get("names", {"p1": "Person 1", "p2": "Person 2"}),
        "custom_cats": SETTINGS.get("custom_cats", []),
        "last_updated": datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M") if p.exists() else None,
        "tx_count": len(txns),
        "statement_counts": {
            "credit":      len(list(_stmts_path("credit").glob("*.*"))),
            "debit":       len(list(_stmts_path("debit").glob("*.*"))),
            "investments": len(list(_stmts_path("investments").glob("*.csv"))),
        }
    })

@app.route("/api/transactions")
def get_transactions():
    month_key = request.args.get("month")
    txns = load_transactions()
    if month_key: txns = [t for t in txns if t["key"] == month_key]
    return jsonify([{
        "tx_key": t["tx_key"], "date": t["date"], "payee": t["payee"],
        "amount": t["amount"], "category": t["category"],
        "original_category": t["original_category"], "source": t["source"],
        "flow": t["flow"], "excluded": t["excluded"], "overridden": t["overridden"],
    } for t in txns])

@app.route("/api/override", methods=["POST"])
def set_override():
    data = request.get_json() or {}; tx_key = data.get("tx_key")
    if not tx_key: return jsonify({"error":"tx_key required"}), 400
    with state_lock:
        if data.get("reset"): OVERRIDES.pop(tx_key, None)
        else:
            ov = OVERRIDES.get(tx_key, {})
            if "category" in data: ov["category"] = data["category"]
            if "exclude"  in data: ov["exclude"]  = bool(data["exclude"])
            OVERRIDES[tx_key] = ov
    save_json(OVERRIDES_FILE, OVERRIDES)
    push_event("data_refreshed", {"ts": datetime.now().isoformat()})
    return jsonify({"ok": True})

@app.route("/api/prices")
def get_prices():
    syms = [s.strip() for s in request.args.get("symbols","").split(",") if s.strip()]
    if not syms: return jsonify({})
    return jsonify(get_stock_prices(syms))

@app.route("/api/settings", methods=["GET"])
def get_settings():
    return jsonify({**SETTINGS, "income_derived": derived_income(SETTINGS)})

@app.route("/api/settings", methods=["POST"])
def update_settings():
    data = request.get_json() or {}
    with state_lock:
        if "income"      in data: SETTINGS["income"].update(data["income"])
        if "limits"      in data: SETTINGS["limits"].update(data["limits"])
        if "names"       in data: SETTINGS["names"] = data["names"]
        if "custom_cats" in data: SETTINGS["custom_cats"] = data["custom_cats"]
        if "new_cat_rule" in data and data["new_cat_rule"]:
            _append_category_rule(data["new_cat_rule"]["keyword"], data["new_cat_rule"]["category"])
    save_json(SETTINGS_FILE, SETTINGS)
    push_event("settings_updated", {"income": derived_income(SETTINGS), "limits": SETTINGS["limits"]})
    return jsonify({**SETTINGS, "income_derived": derived_income(SETTINGS)})

@app.route("/api/plan", methods=["GET"])
def get_plan():
    return jsonify({**PLAN, "projection": compute_projection(PLAN)})

@app.route("/api/plan", methods=["POST"])
def update_plan():
    data = request.get_json() or {}
    with state_lock:
        for k in ["plan_type","sean_interest","casie_interest","ready_by","target_price","down_pct",
                  "mortgage_rate","mortgage_term","sean_start_bal","casie_start_bal",
                  "start_month","sean_default_monthly","casie_default_monthly",
                  "sean_default_income","casie_default_income",
                  "condo_value","condo_rate","condo_mortgage_rate","condo_principle","condo_purchase_price","condo_monthly_principal",
                  "condo_purchase_date","condo_value_date"]:
            if k in data: PLAN[k] = data[k]
        if "months"           in data: PLAN["months"].update(data["months"])
        if "year_incomes"     in data: PLAN["year_incomes"]     = data["year_incomes"]
        if "income_overrides" in data: PLAN["income_overrides"] = data["income_overrides"]
        if "liq_dp_pct"       in data: PLAN["liq_dp_pct"]       = data["liq_dp_pct"]
        if "liq_res_pct"      in data: PLAN["liq_res_pct"]      = data["liq_res_pct"]
    save_json(PLAN_FILE, PLAN)
    return jsonify({**PLAN, "projection": compute_projection(PLAN)})

@app.route("/api/plan/month", methods=["POST"])
def update_plan_month():
    data = request.get_json() or {}; key = data.get("key")
    if not key: return jsonify({"error":"key required"}), 400
    with state_lock:
        ex = PLAN["months"].get(key, {})
        for f in ["sean_income","casie_income","sean_contrib","casie_contrib"]:
            if f in data: ex[f] = float(data[f])
        PLAN["months"][key] = ex
    save_json(PLAN_FILE, PLAN)
    return jsonify({"ok": True, "projection": compute_projection(PLAN)})

if __name__ == "__main__":
    _create_shortcut_once()
    add_log("Financial Planner -> http://localhost:5100", "info")
    # Auto-open browser after a short delay (gives Flask time to start)
    if getattr(sys, 'frozen', False):
        import threading, webbrowser, time
        def _open_browser():
            time.sleep(1.5)
            webbrowser.open("http://localhost:5100")
        threading.Thread(target=_open_browser, daemon=True).start()
    app.run(host="0.0.0.0", port=5100, debug=False, threaded=True)
