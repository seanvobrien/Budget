# src/mapping.py
from __future__ import annotations
import pandas as pd
from typing import Iterable, Tuple

# ── Base mapping ─────────────────────────────────────────────────────────────
# Rules applied in order; first match wins for uncategorized rows.
# Covers a broad set of common US merchants across all major categories.

MAPPING: list[tuple[str, str]] = [

    # ── Grocery ──────────────────────────────────────────────────────────────
    ("Market Basket",       "Grocery"),
    ("Shaws",               "Grocery"),
    ("Shaw's",              "Grocery"),
    ("Stop & Shop",         "Grocery"),
    ("Stop and Shop",       "Grocery"),
    ("Trader Joe",          "Grocery"),
    ("Whole Foods",         "Grocery"),
    ("Wegmans",             "Grocery"),
    ("Kroger",              "Grocery"),
    ("Safeway",             "Grocery"),
    ("Publix",              "Grocery"),
    ("Aldi",                "Grocery"),
    ("Lidl",                "Grocery"),
    ("H-E-B",               "Grocery"),
    ("Meijer",              "Grocery"),
    ("Giant",               "Grocery"),
    ("Hannaford",           "Grocery"),
    ("Price Chopper",       "Grocery"),
    ("Market 32",           "Grocery"),
    ("BJs Wholesale",       "Grocery"),
    ("BJS Wholesale",       "Grocery"),
    ("Costco",              "Grocery"),
    ("Sams Club",           "Grocery"),
    ("Sam's Club",          "Grocery"),
    ("Savas",               "Grocery"),
    ("Kappy",               "Grocery"),
    ("Walgreens",           "Grocery"),
    ("CVS",                 "Grocery"),
    ("Rite Aid",            "Grocery"),
    ("Target",              "Grocery"),
    ("Walmart",             "Grocery"),
    ("Instacart",           "Grocery"),
    ("Fresh Direct",        "Grocery"),
    ("FreshDirect",         "Grocery"),
    ("Peapod",              "Grocery"),
    ("Shipt",               "Grocery"),
    ("DoorDash Grocery",    "Grocery"),

    # ── Restaurant / Dining ──────────────────────────────────────────────────
    ("TST*",                "Restaurant"),
    ("SQ *",                "Restaurant"),
    ("Uber Eats",           "Restaurant"),
    ("DoorDash",            "Restaurant"),
    ("Grubhub",             "Restaurant"),
    ("Seamless",            "Restaurant"),
    ("Postmates",           "Restaurant"),
    ("Caviar",              "Restaurant"),
    ("Dunkin",              "Restaurant"),
    ("Starbucks",           "Restaurant"),
    ("Blank Street",        "Restaurant"),
    ("Aroma Joe",           "Restaurant"),
    ("Honey Dew",           "Restaurant"),
    ("Heavenly Donut",      "Restaurant"),
    ("Panera",              "Restaurant"),
    ("Chipotle",            "Restaurant"),
    ("Shake Shack",         "Restaurant"),
    ("Five Guys",           "Restaurant"),
    ("Sweetgreen",          "Restaurant"),
    ("Cava",                "Restaurant"),
    ("Chick-fil-A",         "Restaurant"),
    ("Chick Fil A",         "Restaurant"),
    ("McDonald",            "Restaurant"),
    ("Burger King",         "Restaurant"),
    ("Wendy",               "Restaurant"),
    ("Taco Bell",           "Restaurant"),
    ("Domino",              "Restaurant"),
    ("Pizza Hut",           "Restaurant"),
    ("Papa John",           "Restaurant"),
    ("Subway",              "Restaurant"),
    ("Cheesecake Factory",  "Restaurant"),
    ("Olive Garden",        "Restaurant"),
    ("Red Lobster",         "Restaurant"),
    ("Applebee",            "Restaurant"),
    ("IHOP",                "Restaurant"),
    ("Denny",               "Restaurant"),
    ("Panda Express",       "Restaurant"),
    ("Wingstop",            "Restaurant"),
    ("Raising Cane",        "Restaurant"),
    ("Toast",               "Restaurant"),
    ("Yelp*",               "Restaurant"),

    # ── Utilities ────────────────────────────────────────────────────────────
    ("Eversource",          "Electric"),
    ("National Grid",       "Natural Gas"),
    ("NGRID06",             "Natural Gas"),
    ("Xcel Energy",         "Electric"),
    ("Con Edison",          "Electric"),
    ("ConEd",               "Electric"),
    ("PG&E",                "Electric"),
    ("Duke Energy",         "Electric"),
    ("Dominion Energy",     "Electric"),
    ("Ameren",              "Electric"),
    ("Nicor Gas",           "Natural Gas"),
    ("Atmos Energy",        "Natural Gas"),
    ("Piedmont Natural Gas","Natural Gas"),
    ("Spire",               "Natural Gas"),
    ("NW Natural",          "Natural Gas"),
    ("Southern Gas",        "Natural Gas"),
    ("Columbia Gas",        "Natural Gas"),
    ("Unitil",              "Electric"),
    ("Avangrid",            "Electric"),

    # ── Internet / Phone ─────────────────────────────────────────────────────
    ("Verizon",             "Internet"),
    ("Comcast",             "Internet"),
    ("Xfinity",             "Internet"),
    ("Spectrum",            "Internet"),
    ("Cox",                 "Internet"),
    ("AT&T",                "Internet"),
    ("T-Mobile",            "Internet"),
    ("Optimum",             "Internet"),
    ("Frontier",            "Internet"),
    ("CenturyLink",         "Internet"),
    ("Lumen",               "Internet"),
    ("Google Fi",           "Internet"),
    ("Mint Mobile",         "Internet"),
    ("Visible",             "Internet"),

    # ── Mortgage / Housing ───────────────────────────────────────────────────
    ("East Cambridge",      "Mortgage"),
    ("Electronic Funds",    "HOA"),
    ("HOA",                 "HOA"),

    # ── Car ──────────────────────────────────────────────────────────────────
    ("Hav. Fire Dpt",       "Car Payment"),
    ("Toyota Financial",    "Car Payment"),
    ("Honda Financial",     "Car Payment"),
    ("Ford Motor Credit",   "Car Payment"),
    ("GM Financial",        "Car Payment"),
    ("Hyundai Motor",       "Car Payment"),
    ("Kia Motor",           "Car Payment"),
    ("Ally Financial",      "Car Payment"),
    ("Capital One Auto",    "Car Payment"),
    ("Chase Auto",          "Car Payment"),
    ("BJS Fuel",            "Car Services"),
    ("Exxon",               "Car Services"),
    ("Mobil",               "Car Services"),
    ("Shell",               "Car Services"),
    ("BP",                  "Car Services"),
    ("Chevron",             "Car Services"),
    ("Gulf",                "Car Services"),
    ("Sunoco",              "Car Services"),
    ("Cumberland Farms",    "Car Services"),
    ("GetGo",               "Car Services"),
    ("Speedway",            "Car Services"),
    ("Circle K",            "Car Services"),
    ("Wawa",                "Car Services"),
    ("Sheetz",              "Car Services"),
    ("QuikTrip",            "Car Services"),
    ("IRA Toyota",          "Car Services"),
    ("E-Zpass",             "Car Services"),
    ("EZ Pass",             "Car Services"),
    ("Squikee Clean",       "Car Services"),
    ("Jiffy Lube",          "Car Services"),
    ("Pep Boys",            "Car Services"),
    ("AutoZone",            "Car Services"),
    ("O'Reilly Auto",       "Car Services"),
    ("Advance Auto",        "Car Services"),
    ("Midas",               "Car Services"),
    ("Firestone",           "Car Services"),
    ("Mavis",               "Car Services"),

    # ── Health & Fitness ─────────────────────────────────────────────────────
    ("Beacon Hill",         "Gym"),
    ("Planet Fitness",      "Gym"),
    ("Equinox",             "Gym"),
    ("LA Fitness",          "Gym"),
    ("24 Hour Fitness",     "Gym"),
    ("Gold's Gym",          "Gym"),
    ("Crunch",              "Gym"),
    ("Anytime Fitness",     "Gym"),
    ("SoulCycle",           "Gym"),
    ("Peloton",             "Gym"),
    ("ClassPass",           "Gym"),
    ("Orange Theory",       "Gym"),
    ("F45",                 "Gym"),
    ("Barry",               "Gym"),
    ("Pure Barre",          "Gym"),
    ("YMCA",                "Gym"),

    # ── Transit ──────────────────────────────────────────────────────────────
    ("MBTA",                "MBTA"),
    ("Charlie Card",        "MBTA"),
    ("MTA",                 "MBTA"),
    ("BART",                "MBTA"),
    ("Metra",               "MBTA"),
    ("NJ Transit",          "MBTA"),
    ("Metro Transit",       "MBTA"),
    ("Clipper",             "MBTA"),
    ("SmarTrip",            "MBTA"),
    ("Amtrak",              "MBTA"),

    # ── Rideshare ────────────────────────────────────────────────────────────
    ("Uber",                "Uber"),
    ("Lyft",                "Uber"),

    # ── Amazon ───────────────────────────────────────────────────────────────
    ("Amazon",              "Amazon"),
    ("AMZ",                 "Amazon"),
    ("AMZN",                "Amazon"),
    ("Whole Foods",         "Amazon"),   # owned by Amazon — keep after Grocery check

    # ── Subscriptions ────────────────────────────────────────────────────────
    ("Apple.com",           "Subscription"),
    ("Apple One",           "Subscription"),
    ("iTunes",              "Subscription"),
    ("Spotify",             "Subscription"),
    ("Netflix",             "Subscription"),
    ("Hulu",                "Subscription"),
    ("Disney",              "Subscription"),
    ("HBO",                 "Subscription"),
    ("Max",                 "Subscription"),
    ("Paramount",           "Subscription"),
    ("Peacock",             "Subscription"),
    ("YouTube Premium",     "Subscription"),
    ("Google One",          "Subscription"),
    ("Google Play",         "Subscription"),
    ("Microsoft 365",       "Subscription"),
    ("Adobe",               "Subscription"),
    ("Dropbox",             "Subscription"),
    ("iCloud",              "Subscription"),
    ("Audible",             "Subscription"),
    ("Kindle",              "Subscription"),
    ("SiriusXM",            "Subscription"),
    ("Pandora",             "Subscription"),
    ("Tidal",               "Subscription"),
    ("LinkedIn",            "Subscription"),
    ("Duolingo",            "Subscription"),
    ("NY Times",            "Subscription"),
    ("New York Times",      "Subscription"),
    ("Wall Street Journal", "Subscription"),
    ("Washington Post",     "Subscription"),
    ("The Atlantic",        "Subscription"),
    ("Calm",                "Subscription"),
    ("Headspace",           "Subscription"),
    ("Weight Watchers",     "Subscription"),
    ("Noom",                "Subscription"),
    ("Strava",              "Subscription"),
    ("Zwift",               "Subscription"),

    # ── Insurance ────────────────────────────────────────────────────────────
    ("Norfolk & Dedham",    "Insurance"),
    ("Lemonade",            "Insurance"),
    ("Progressive",         "Insurance"),
    ("GEICO",               "Insurance"),
    ("State Farm",          "Insurance"),
    ("Allstate",            "Insurance"),
    ("Liberty Mutual",      "Insurance"),
    ("Nationwide",          "Insurance"),
    ("Farmers",             "Insurance"),
    ("USAA",                "Insurance"),
    ("Amica",               "Insurance"),
    ("MetLife",             "Insurance"),
    ("Travelers",           "Insurance"),
    ("Cigna",               "Insurance"),
    ("Aetna",               "Insurance"),
    ("Blue Cross",          "Insurance"),
    ("Anthem",              "Insurance"),
    ("Humana",              "Insurance"),
    ("UnitedHealth",        "Insurance"),

    # ── Veterinary & Pets ────────────────────────────────────────────────────
    ("Newburyport Veterinary","Veterinary"),
    ("Chewy",               "Veterinary"),
    ("PetSmart",            "Veterinary"),
    ("Petco",               "Veterinary"),
    ("Banfield",            "Veterinary"),
    ("VCA",                 "Veterinary"),
    ("BluePearl",           "Veterinary"),
    ("Pet Supplies Plus",   "Veterinary"),

    # ── Dental / Medical ─────────────────────────────────────────────────────
    ("Newton Dental",       "Dentist Office"),
    ("Dental",              "Dentist Office"),
    ("Orthodont",           "Dentist Office"),
    ("Oral",                "Dentist Office"),

    # ── Income / Transfers ───────────────────────────────────────────────────
    ("Direct Deposit",      "Deposit"),
    ("Check Received",      "Deposit"),
    ("Direct Debit",        "Payment"),
    ("Check Paid",          "Payment"),
    ("Transferred",         "Contribution"),
    ("Cash Advance",        "ATM"),
    ("ATM Withdrawal",      "ATM"),
    ("Venmo",               "Venmo"),
    ("Zelle",               "Venmo"),
    ("PayPal",              "Venmo"),
    ("Cash App",            "Venmo"),
    ("Apple Cash",          "Venmo"),

    # ── Home Improvement ─────────────────────────────────────────────────────
    ("Home Depot",          "All-Other"),
    ("Lowe",                "All-Other"),
    ("Ace Hardware",        "All-Other"),
    ("Menards",             "All-Other"),
    ("IKEA",                "All-Other"),
    ("Wayfair",             "All-Other"),
    ("Williams-Sonoma",     "All-Other"),
    ("Crate & Barrel",      "All-Other"),
    ("West Elm",            "All-Other"),

    # ── Retail / Shopping ────────────────────────────────────────────────────
    ("Best Buy",            "All-Other"),
    ("Apple Store",         "All-Other"),
    ("Nike",                "All-Other"),
    ("Adidas",              "All-Other"),
    ("TJ Maxx",             "All-Other"),
    ("Marshalls",           "All-Other"),
    ("Ross",                "All-Other"),
    ("Old Navy",            "All-Other"),
    ("Gap",                 "All-Other"),
    ("H&M",                 "All-Other"),
    ("Zara",                "All-Other"),
    ("Uniqlo",              "All-Other"),
    ("Nordstrom",           "All-Other"),
    ("Macy",                "All-Other"),
    ("Kohl",                "All-Other"),
    ("J.Crew",              "All-Other"),
    ("Banana Republic",     "All-Other"),
    ("Express",             "All-Other"),
    ("Bath & Body",         "All-Other"),
    ("Sephora",             "All-Other"),
    ("Ulta",                "All-Other"),
    ("Etsy",                "All-Other"),
]

# ── High-priority rules (override everything) ────────────────────────────────
# Checked first — these win regardless of order above.

HIGH_PRIORITY: list[tuple[str, str]] = [
    ("LAURA PRESHONG",              "Exclude"),
    ("ACH Deposit",                 "Exclude"),
    ("Direct Deposit SharkNinja",   "Pay Check"),
    ("National Grid",               "Natural Gas"),
    ("Eversource",                  "Electric"),
    ("Venmo",                       "Venmo"),
]


def apply_mapping(
    df: pd.DataFrame,
    mapping: Iterable[Tuple[str, str]] = MAPPING,
    high_priority: Iterable[Tuple[str, str]] = HIGH_PRIORITY,
) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    if "Category" not in out.columns:
        out["Category"] = "All-Other"

    # 1. High-priority overrides — run first, unconditionally
    for needle, cat in high_priority:
        mask = out["Payee"].str.contains(needle, case=False, na=False, regex=False)
        out.loc[mask, "Category"] = cat

    # 2. Base mapping — only fills rows still at All-Other
    hi_needles = {n.lower() for n, _ in high_priority}
    base_mapping = [(n, c) for (n, c) in mapping if n.lower() not in hi_needles]

    for needle, cat in base_mapping:
        mask = (
            out["Category"].eq("All-Other") &
            out["Payee"].str.contains(needle, case=False, na=False, regex=False)
        )
        out.loc[mask, "Category"] = cat

    # 3. Source-based fallback — Fidelity/Investment rows still uncategorized
    mask_invest = (
        out["Category"].eq("All-Other") &
        out["Source"].str.contains("Fidelity|Investment|Brokerage", case=False, na=False)
    )
    out.loc[mask_invest, "Category"] = "Investment"

    return out
