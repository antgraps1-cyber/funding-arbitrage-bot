#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║        CASH & CARRY ARBITRAGE SCANNER  —  INDIAN F&O MARKETS                 ║
║        AngelOne Smart API  |  Read-Only  |  Real NSE Data                    ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  DATA SOURCES:                                                               ║
║  1. AngelOneProvider — Angel One Smart API (real NSE/NFO data)               ║
║  2. MockProvider     — Built-in fake data  (offline / no creds needed)       ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  INSTALL (one-time):                                                         ║
║    pip install requests pyotp rich                                           ║
║                                                                              ║
║  CREDENTIALS — fill in ANGELONE_CREDS below OR pass via CLI:                 ║
║    --api-key   YOUR_API_KEY                                                  ║
║    --client-id YOUR_CLIENT_CODE                                              ║
║    --password  YOUR_PIN_OR_PASSWORD                                          ║
║    --totp-key  YOUR_TOTP_SECRET   (the secret shown when enabling 2FA)       ║
║                                                                              ║
║  RUN:                                                                        ║
║    python arb_scanner.py                      # live AngelOne data           ║
║    python arb_scanner.py --provider mock      # offline test                 ║
║    python arb_scanner.py --once               # single scan then exit        ║
║    python arb_scanner.py --min-ann 12         # only show >12% annualized    ║
║    python arb_scanner.py --no-market-check    # scan any time (testing)      ║
╚══════════════════════════════════════════════════════════════════════════════╝

STRATEGY: Cash & Carry Arbitrage (Spot–Future Arbitrage)
  BUY spot stock  +  SELL near-month futures
  Lock-in spread = (Futures - Spot) / Spot × 100%
  Hold to expiry → prices converge → profit = net spread after costs
"""

# ─────────────────────────────────────────────────────────────────────────────
# STDLIB
# ─────────────────────────────────────────────────────────────────────────────
import argparse, csv, json, math, os, random, sys, time, threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import zoneinfo

# IST — the ONLY source of truth for all time comparisons
try:
    IST = zoneinfo.ZoneInfo("Asia/Kolkata")
except Exception:
    IST = timezone(timedelta(hours=5, minutes=30))

def now_ist() -> datetime:
    return datetime.now(tz=IST)


# ─────────────────────────────────────────────────────────────────────────────
# OPTIONAL DEPS
# ─────────────────────────────────────────────────────────────────────────────
try:
    import requests
    REQUESTS_OK = True
except ImportError:
    REQUESTS_OK = False
    print("[WARN] 'requests' not installed. Run: pip install requests")

try:
    import pyotp
    PYOTP_OK = True
except ImportError:
    PYOTP_OK = False

try:
    from rich.console import Console
    from rich.table import Table
    from rich.live import Live
    from rich.panel import Panel
    from rich.text import Text
    from rich import box
    from rich.console import Group
    RICH_OK = True
except ImportError:
    RICH_OK = False
    print("[TIP] Run  pip install rich  for a colour dashboard.\n")


# ─────────────────────────────────────────────────────────────────────────────
# ══ ANGELONE CREDENTIALS ══════════════════════════════════════════════════════
#  Fill these in, OR pass them via CLI flags (--api-key, --client-id, etc.)
#  Leave as empty strings "" if you prefer the CLI approach.
# ─────────────────────────────────────────────────────────────────────────────
ANGELONE_CREDS = {
    "API_KEY":   "gX5GU2nS",          # e.g. "abc123XYZ"
    "CLIENT_ID": "B51721058",          # your AngelOne login ID / client code
    "PASSWORD":  "2801",          # your AngelOne MPIN / password
    "TOTP_KEY":  "BLLJSREYBFHFDLTKERHIONSF5A",          # TOTP secret (shown when you set up 2FA in AngelOne app)
                              # If 2FA is disabled on your account, leave blank
}


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
CONFIG = {
    # ── Scanner ──────────────────────────────────────────────────────────────
    "SCAN_INTERVAL_SECONDS":   60,
    "TOP_N_DISPLAY":           20,
    "MIN_SPREAD_PCT":          4.0,    # entry: raw spread must be > 4%
    "MAX_SPREAD_PCT":          15.0,   # ignore if > 15% (likely bad data)
    "MIN_DAYS_TO_EXPIRY":      3,
    "MAX_DAYS_TO_EXPIRY":      35,

    # ── Cost model (Indian market accurate) ──────────────────────────────────
    "BROKERAGE_PER_ORDER_RS":  20,     # flat ₹20/order (4 legs)
    "STT_SELL_DELIVERY_PCT":   0.1,    # STT on spot sell
    "STT_SELL_FUTURES_PCT":    0.01,   # STT on futures sell
    "EXCHANGE_TXN_PCT":        0.00345,
    "SEBI_FEE_PCT":            0.0001,
    "STAMP_DUTY_PCT":          0.015,
    "GST_PCT":                 18,
    "OVERALL_SLIPPAGE_PCT":    0.3,    # 0.3% round-trip slippage all 4 legs

    # ── Paper trading ────────────────────────────────────────────────────────
    "PAPER_CAPITAL_RS":        1_500_000,
    "MAX_POSITION_SIZE_RS":    300_000,
    "MAX_OPEN_POSITIONS":      5,
    "FUTURES_MARGIN_PCT":      15.0,

    # ── Risk management ──────────────────────────────────────────────────────
    "STOP_LOSS_SPREAD_PCT":    -0.5,
    "MAX_DAILY_LOSS_RS":       5_000,
    "MIN_ANNUALIZED_PCT":      0.0,

    # ── Switching logic ──────────────────────────────────────────────────────
    "SWITCH_THRESHOLD_PCT":    0.50,
    "SWITCH_COOLDOWN_SEC":     300,

    # ── Logging ──────────────────────────────────────────────────────────────
    "LOG_DIR":    "./arb_logs",
    "SCAN_CSV":   "scans.csv",
    "TRADES_CSV": "trades.csv",
    "PNL_CSV":    "pnl.csv",

    # ── AngelOne API ──────────────────────────────────────────────────────────
    "AO_TIMEOUT":           12,    # HTTP timeout seconds
    "AO_RETRY":             2,     # retries on failure
    "AO_TOKEN_TTL":         82800, # re-login after 23 hours (tokens valid ~24h)

    # ── Market hours (IST) ────────────────────────────────────────────────────
    "MARKET_OPEN_HOUR":     9,
    "MARKET_OPEN_MIN":      15,
    "MARKET_CLOSE_HOUR":    15,
    "MARKET_CLOSE_MIN":     30,
    "SKIP_MARKET_HOURS_CHECK": False,
}

# ─────────────────────────────────────────────────────────────────────────────
# MARKET HOURS HELPER  (IST-aware — was the original bug)
# ─────────────────────────────────────────────────────────────────────────────
NSE_HOLIDAYS_2025 = {
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10",
    "2025-04-14", "2025-04-18", "2025-05-01", "2025-08-15",
    "2025-08-27", "2025-10-02", "2025-10-20", "2025-10-24",
    "2025-11-05", "2025-12-25",
}
NSE_HOLIDAYS_2026 = {
    "2026-01-26", "2026-03-03", "2026-03-20", "2026-04-02",
    "2026-04-03", "2026-04-14", "2026-05-01", "2026-06-19",
    "2026-08-15", "2026-10-02", "2026-11-23", "2026-12-25",
}
ALL_HOLIDAYS = NSE_HOLIDAYS_2025 | NSE_HOLIDAYS_2026

def is_market_open(cfg: dict) -> Tuple[bool, str]:
    if cfg.get("SKIP_MARKET_HOURS_CHECK", False):
        return True, "BYPASSED (--no-market-check)"

    t        = now_ist()
    date_str = t.strftime("%Y-%m-%d")

    if t.weekday() >= 5:
        return False, f"Weekend ({t.strftime('%A')}) — NSE closed"

    if date_str in ALL_HOLIDAYS:
        return False, f"NSE Holiday ({date_str})"

    oh, om = cfg["MARKET_OPEN_HOUR"],  cfg["MARKET_OPEN_MIN"]
    ch, cm = cfg["MARKET_CLOSE_HOUR"], cfg["MARKET_CLOSE_MIN"]
    open_t  = t.replace(hour=oh, minute=om, second=0, microsecond=0)
    close_t = t.replace(hour=ch, minute=cm, second=0, microsecond=0)

    if t < open_t:
        mins = int((open_t - t).total_seconds() // 60)
        return False, f"Pre-market — opens in {mins} min (IST {oh:02d}:{om:02d})"
    if t > close_t:
        return False, f"After-hours — NSE closed at {ch:02d}:{cm:02d} IST"

    return True, f"OPEN — IST {t.strftime('%H:%M:%S')}"


# ─────────────────────────────────────────────────────────────────────────────
# F&O STOCK UNIVERSE
# ─────────────────────────────────────────────────────────────────────────────
FNO_UNIVERSE = [
    "RELIANCE","TCS","INFY","HDFCBANK","ICICIBANK","HINDUNILVR","KOTAKBANK",
    "SBIN","BAJFINANCE","BHARTIARTL","ITC","ASIANPAINT","AXISBANK","MARUTI",
    "TITAN","NESTLEIND","ULTRACEMCO","TECHM","HCLTECH","WIPRO","SUNPHARMA",
    "DIVISLAB","DRREDDY","CIPLA","TATAMOTORS","M&M","BAJAJFINSV","ONGC",
    "POWERGRID","NTPC","COALINDIA","BPCL","IOC","GAIL","ADANIPORTS","ADANIENT",
    "ADANIGREEN","TATAPOWER","DMART","BAJAJ-AUTO","HEROMOTOCO","EICHERMOT",
    "BRITANNIA","HAVELLS","DABUR","MARICO","GODREJCP","COLPAL","TATACONSUM",
    "PIDILITIND","BERGERPAINTS","SIEMENS","ABB","VOLTAS","POLYCAB","CUMMINSIND",
    "BOSCHLTD","TATASTEEL","JSWSTEEL","SAIL","HINDALCO","VEDL","NATIONALUM",
    "HINDZINC","CONCOR","IRCTC","INDIGO","GMRINFRA","TORNTPOWER","CESC",
    "NHPC","SJVN","RECLTD","PFC","IRFC","HDFCLIFE","SBILIFE","ICICIPRULI",
    "LICI","BAJAJHLDNG","CHOLAFIN","MUTHOOTFIN","PNBHOUSING","CANFINHOME",
    "AUBANK","FEDERALBNK","IDFCFIRSTB","RBLBANK","BANDHANBNK","MANAPPURAM",
    "INFOEDGE","ZOMATO","NAUKRI","DELHIVERY","MPHASIS","PERSISTENT","LTTS",
    "COFORGE","HAPPYMNDS","KPITTECH","TRENT","PVRINOX","ZEEL","SUNTV",
    "DEEPAKNITR","AARTIIND","TATACHEMICALS","GUJGASLTD","IGL","MGL","PETRONET",
    "CHAMBLFERT","COROMANDEL","UPL","APOLLOHOSP","APOLLOTYRE","EXIDEIND",
    "AMARARAJA","BALKRISIND","MRF","JKTYRE","BHARATFORG",
    "LALPATHLAB","METROPOLIS","ALKEM","TORNTPHARM","GRANULES","BIOCON",
    "LAURUSLABS","IPCA","NATCOPHARMA","ZYDUSLIFE","GLAND","ABBOTINDIA",
    "SHRIRAMFIN","LTFH","MOTILALOFS","ANGELONE","BSE","MCX","CDSL",
    "GNFC","NOCIL","RATNAMANI","SONACOMS","KAYNES","SHOPERSTOP","WHIRLPOOL",
    "PAGEIND","MCDOWELL-N","UBL","EMAMILTD","CROMPTON","BLUESTARCO","ATGL",
    "NYKAA","PAYTM","POLICYBZR","EASEMYTRIP","INDIAMART","JUSTDIAL","AFFLE",
]
FNO_UNIVERSE = list(dict.fromkeys(FNO_UNIVERSE))

# Official NSE lot sizes (update quarterly)
NSE_LOT_SIZES: Dict[str, int] = {
    "RELIANCE":250,"TCS":150,"INFY":300,"HDFCBANK":550,"ICICIBANK":1375,
    "HINDUNILVR":300,"KOTAKBANK":400,"SBIN":1500,"BAJFINANCE":125,
    "BHARTIARTL":1851,"ITC":3200,"ASIANPAINT":300,"AXISBANK":1200,
    "MARUTI":100,"TITAN":375,"NESTLEIND":50,"ULTRACEMCO":100,
    "TECHM":600,"HCLTECH":700,"WIPRO":1500,"SUNPHARMA":700,
    "DIVISLAB":200,"DRREDDY":125,"CIPLA":650,"TATAMOTORS":2850,
    "M&M":700,"BAJAJFINSV":500,"ONGC":3850,"POWERGRID":4700,
    "NTPC":5750,"COALINDIA":4200,"BPCL":1800,"IOC":4750,"GAIL":3850,
    "ADANIPORTS":1250,"ADANIENT":500,"ADANIGREEN":500,"TATAPOWER":6750,
    "DMART":187,"BAJAJ-AUTO":250,"HEROMOTOCO":300,"EICHERMOT":200,
    "BRITANNIA":200,"HAVELLS":1000,"DABUR":1250,"MARICO":1400,
    "GODREJCP":500,"COLPAL":500,"TATACONSUM":1200,"PIDILITIND":500,
    "BERGERPAINTS":1100,"SIEMENS":275,"ABB":250,"VOLTAS":1000,
    "POLYCAB":400,"CUMMINSIND":600,"BOSCHLTD":50,"TATASTEEL":3500,
    "JSWSTEEL":1350,"SAIL":7000,"HINDALCO":2150,"VEDL":3000,
    "NATIONALUM":8500,"HINDZINC":2700,"CONCOR":2000,"IRCTC":2400,
    "INDIGO":600,"GMRINFRA":11250,"NHPC":15000,"SJVN":12500,
    "RECLTD":3000,"PFC":3500,"IRFC":7000,"HDFCLIFE":1100,
    "SBILIFE":750,"ICICIPRULI":1500,"LICI":700,"CHOLAFIN":500,
    "MUTHOOTFIN":750,"AUBANK":1000,"FEDERALBNK":10000,"IDFCFIRSTB":10000,
    "RBLBANK":5000,"BANDHANBNK":5000,"MANAPPURAM":4000,"INFOEDGE":300,
    "ZOMATO":3750,"NAUKRI":300,"MPHASIS":400,"PERSISTENT":250,
    "LTTS":200,"COFORGE":200,"TRENT":475,"PVRINOX":1000,
    "ZEEL":3000,"SUNTV":1400,"DEEPAKNITR":750,"AARTIIND":1300,
    "TATACHEMICALS":1000,"GUJGASLTD":1000,"IGL":2750,"MGL":550,
    "PETRONET":3000,"CHAMBLFERT":2600,"COROMANDEL":500,"UPL":1300,
    "APOLLOHOSP":300,"APOLLOTYRE":3500,"EXIDEIND":5400,"AMARARAJA":1100,
    "BALKRISIND":400,"MRF":10,"BHARATFORG":1200,
    "SHRIRAMFIN":500,"LTFH":8000,"MOTILALOFS":400,"ANGELONE":350,
    "BSE":400,"MCX":250,"CDSL":2500,"GNFC":2100,"BIOCON":2500,
    "LAURUSLABS":1800,"IPCA":500,"ZYDUSLIFE":700,"GLAND":350,
    "ALKEM":300,"TORNTPHARM":500,"METROPOLIS":400,"LALPATHLAB":300,
}
DEFAULT_LOT = 500

# Mock spot prices for offline testing
MOCK_SPOT: Dict[str, float] = {
    "RELIANCE":2850,"TCS":3920,"INFY":1750,"HDFCBANK":1720,"ICICIBANK":1270,
    "HINDUNILVR":2430,"KOTAKBANK":1900,"SBIN":820,"BAJFINANCE":6750,
    "BHARTIARTL":1580,"ITC":465,"ASIANPAINT":3100,"AXISBANK":1180,
    "MARUTI":12500,"TITAN":3650,"NESTLEIND":24800,"TATAMOTORS":1020,
    "M&M":3200,"WIPRO":550,"HCLTECH":1680,"TECHM":1620,"SUNPHARMA":1680,
    "DRREDDY":6200,"CIPLA":1490,"BAJAJFINSV":1700,"ONGC":265,"NTPC":355,
    "COALINDIA":470,"BPCL":320,"GAIL":215,"ADANIPORTS":1350,"DMART":4800,
    "BAJAJ-AUTO":9800,"HEROMOTOCO":5200,"EICHERMOT":5100,"BRITANNIA":4800,
    "HAVELLS":1850,"DABUR":530,"TATACONSUM":820,"TATAPOWER":430,
    "CONCOR":1050,"IRCTC":860,"INDIGO":4800,"RECLTD":550,"PFC":490,
    "IRFC":175,"HDFCLIFE":680,"SBILIFE":1600,"ZOMATO":260,"MPHASIS":3100,
    "PERSISTENT":5200,"LTTS":5400,"COFORGE":6800,"TRENT":5100,
}


# ─────────────────────────────────────────────────────────────────────────────
# DATA CLASSES
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class Quote:
    symbol:   str
    spot:     float
    futures:  float
    lot_size: int
    expiry:   str    # "29-May-2025"
    days:     int
    source:   str

@dataclass
class Opportunity:
    symbol:         str
    spot:           float
    futures:        float
    raw_spread_pct: float
    cost_pct:       float
    net_spread_pct: float
    ann_pct:        float
    days:           int
    lot_size:       int
    margin_rs:      float
    profit_per_lot: float
    expiry:         str
    timestamp:      str
    signal:         str   # STRONG BUY | BUY | WATCH | SKIP
    source:         str
    rank:           int = 0

@dataclass
class Position:
    pid:              str
    symbol:           str
    entry_spot:       float
    entry_fut:        float
    entry_spread_pct: float
    net_spread_pct:   float
    lots:             int
    lot_size:         int
    capital_rs:       float
    entry_time:       str
    expiry:           str
    days_at_entry:    int
    status:           str           = "OPEN"
    exit_spot:        Optional[float] = None
    exit_fut:         Optional[float] = None
    exit_time:        Optional[str]   = None
    exit_reason:      str           = ""
    realized_pnl:     float         = 0.0
    unrealized_pnl:   float         = 0.0

@dataclass
class Portfolio:
    capital:   float
    cash:      float = 0.0
    deployed:  float = 0.0
    total_pnl: float = 0.0
    daily_pnl: float = 0.0
    trades:    int   = 0
    wins:      int   = 0
    positions: dict  = field(default_factory=dict)
    scans:     int   = 0
    def __post_init__(self): self.cash = self.capital


# ─────────────────────────────────────────────────────────────────────────────
# COST CALCULATOR
# ─────────────────────────────────────────────────────────────────────────────
class CostCalc:
    def __init__(self, cfg: dict):
        self.cfg = cfg

    def total_cost_pct(self, spot: float, fut: float, lot: int, lots: int = 1) -> float:
        """Full round-trip cost as % of spot value (Indian market accurate)."""
        sv    = spot * lot * lots
        fv    = fut  * lot * lots
        brok  = self.cfg["BROKERAGE_PER_ORDER_RS"] * 4        # 4 legs
        gst   = brok * self.cfg["GST_PCT"] / 100
        stt_s = sv   * self.cfg["STT_SELL_DELIVERY_PCT"] / 100
        stt_f = fv   * self.cfg["STT_SELL_FUTURES_PCT"]  / 100
        exc   = (sv + fv) * 2 * self.cfg["EXCHANGE_TXN_PCT"] / 100
        sebi  = (sv + fv) * 2 * self.cfg["SEBI_FEE_PCT"]     / 100
        stamp = sv   * self.cfg["STAMP_DUTY_PCT"] / 100
        slip  = sv   * self.cfg["OVERALL_SLIPPAGE_PCT"] / 100
        total = brok + gst + stt_s + stt_f + exc + sebi + stamp + slip
        return round(total / sv * 100, 4)

    def net_spread(self, raw: float, cost: float) -> float:
        return round(raw - cost, 4)

    def annualized(self, net: float, days: int) -> float:
        return round((net / days) * 365, 2) if days > 0 else 0.0

    def margin_required(self, fut: float, lot: int, lots: int = 1) -> float:
        return round(fut * lot * lots * self.cfg["FUTURES_MARGIN_PCT"] / 100, 0)

    def max_profit_per_lot(self, spot: float, net_pct: float, lot: int) -> float:
        return round(spot * lot * net_pct / 100, 0)


# ─────────────────────────────────────────────────────────────────────────────
# PROVIDER 1 — ANGEL ONE SMART API
# ─────────────────────────────────────────────────────────────────────────────
class AngelOneProvider:
    """
    Uses AngelOne Smart API (read-only) for real NSE spot + NFO futures data.

    HOW IT WORKS:
    ┌─────────────────────────────────────────────────────────────────────┐
    │ 1. On startup, download the AngelOne instrument master JSON         │
    │    (no auth needed) — maps symbol names → token numbers             │
    │ 2. Login with API key + client code + password + TOTP               │
    │    → get jwtToken (valid ~24h, auto-refreshed here)                 │
    │ 3. For each symbol, call the LTP quote endpoint:                    │
    │      NSE segment  → spot price                                      │
    │      NFO segment  → near-month futures price + expiry               │
    └─────────────────────────────────────────────────────────────────────┘

    ENDPOINTS USED:
      POST /rest/auth/angelbroking/user/v1/loginByPassword  → login
      POST /rest/secure/angelbroking/market/v1/quote/       → LTP quotes
      GET  https://margincalculator.angelbroking.com/       → scrip master

    RATE LIMIT: ~3 req/s on free tier (handled with AO_DELAY below).
    """

    BASE_URL    = "https://apiconnect.angelone.in"
    MASTER_URL  = ("https://margincalculator.angelbroking.com"
                   "/OpenAPI_File/files/OpenAPIScripMaster.json")
    AO_DELAY    = 0.35   # seconds between API calls (stay under rate limit)

    def __init__(self, cfg: dict, api_key: str, client_id: str,
                 password: str, totp_key: str):
        if not REQUESTS_OK:
            raise RuntimeError("pip install requests")
        self.cfg       = cfg
        self.api_key   = api_key
        self.client_id = client_id
        self.password  = password
        self.totp_key  = totp_key.strip() if totp_key else ""

        self._jwt:       str   = ""
        self._feed_tok:  str   = ""
        self._login_ts:  float = 0.0
        self._lock               = threading.Lock()

        # symbol → {"nse_token": "...", "nfo_tokens": [...]}
        # nfo_tokens = list of {"token","expiry","lot_size"}
        self._instrument_map: Dict[str, dict] = {}

        self._load_instrument_master()
        self._login()

    # ── Instrument master ────────────────────────────────────────────────────
    def _load_instrument_master(self):
        """
        Download AngelOne scrip master and build a lookup:
          self._instrument_map[SYMBOL] = {
              "nse_token":  "2885",      # NSE EQ token
              "nfo_futures": [           # sorted near-month first
                  {"token": "58662", "expiry": "29-May-2025", "lot": 250},
                  ...
              ]
          }
        """
        print("[AngelOne] Downloading instrument master (one-time)...")
        try:
            r = requests.get(self.MASTER_URL,
                             timeout=self.cfg["AO_TIMEOUT"])
            r.raise_for_status()
            master = r.json()
        except Exception as e:
            print(f"[AngelOne] WARNING: Could not load instrument master: {e}")
            print("[AngelOne] Continuing without master — tokens must be hardcoded.")
            master = []

        # Build lookup
        nse_eq: Dict[str, str] = {}      # symbol → token
        nfo_fut: Dict[str, list] = {}    # symbol → [{token, expiry, lot}]

        for item in master:
            exch    = item.get("exch_seg", "")
            itype   = item.get("instrumenttype", "")
            symbol  = item.get("symbol", "")
            name    = item.get("name", "").upper().strip()
            token   = item.get("token", "")
            lot     = int(item.get("lotsize") or DEFAULT_LOT)
            expiry  = item.get("expiry", "")  # format: "29MAY2025"

            # NSE Equity
            if exch == "NSE" and itype == "":
                # symbol like "RELIANCE-EQ"
                clean = name or symbol.replace("-EQ", "").upper().strip()
                if clean in FNO_UNIVERSE and clean not in nse_eq:
                    nse_eq[clean] = token

            # NFO Stock Futures
            elif exch == "NFO" and itype == "FUTSTK":
                clean = name.upper().strip()
                if clean in FNO_UNIVERSE:
                    exp_str = self._parse_expiry(expiry)
                    if exp_str:
                        nfo_fut.setdefault(clean, []).append({
                            "token":  token,
                            "expiry": exp_str,
                            "lot":    NSE_LOT_SIZES.get(clean, lot),
                        })

        # Sort futures by expiry and keep only those within range
        today = now_ist().date()
        for sym, contracts in nfo_fut.items():
            def sort_key(c):
                try:
                    return (datetime.strptime(c["expiry"], "%d-%b-%Y").date()
                            - today).days
                except Exception:
                    return 9999
            contracts.sort(key=sort_key)
            # Filter to valid range
            valid = [c for c in contracts
                     if self.cfg["MIN_DAYS_TO_EXPIRY"]
                        <= sort_key(c)
                        <= self.cfg["MAX_DAYS_TO_EXPIRY"]]
            if valid:
                self._instrument_map[sym] = {
                    "nse_token":   nse_eq.get(sym, ""),
                    "nfo_futures": valid,
                }

        found = len(self._instrument_map)
        print(f"[AngelOne] Instrument master loaded: "
              f"{found} F&O symbols mapped (NSE+NFO)")

    @staticmethod
    def _parse_expiry(raw: str) -> Optional[str]:
        """Convert '29MAY2025' → '29-May-2025'."""
        raw = raw.strip().upper()
        for fmt in ("%d%b%Y", "%d-%b-%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(raw, fmt).strftime("%d-%b-%Y")
            except Exception:
                pass
        return None

    # ── Auth ─────────────────────────────────────────────────────────────────
    def _generate_totp(self) -> str:
        if not self.totp_key:
            return ""
        if not PYOTP_OK:
            raise RuntimeError(
                "pyotp not installed. Run: pip install pyotp\n"
                "Or disable 2FA on your AngelOne account.")
        return pyotp.TOTP(self.totp_key).now()

    def _login(self):
        """Login to AngelOne Smart API and store JWT token."""
        totp = self._generate_totp()
        payload = {
            "clientcode": self.client_id,
            "password":   self.password,
        }
        if totp:
            payload["totp"] = totp

        headers = self._base_headers()
        headers["Content-Type"] = "application/json"

        for attempt in range(self.cfg["AO_RETRY"] + 1):
            try:
                r = requests.post(
                    f"{self.BASE_URL}/rest/auth/angelbroking/user/v1/loginByPassword",
                    json=payload,
                    headers=headers,
                    timeout=self.cfg["AO_TIMEOUT"],
                )
                data = r.json()
                if data.get("status") and data.get("data"):
                    d = data["data"]
                    self._jwt      = d.get("jwtToken", "")
                    self._feed_tok = d.get("feedToken", "")
                    self._login_ts = time.time()
                    print(f"[AngelOne] Login OK — "
                          f"client={self.client_id}  "
                          f"token={self._jwt[:20]}...")
                    return
                else:
                    err = data.get("message", "Unknown error")
                    print(f"[AngelOne] Login attempt {attempt+1} failed: {err}")
            except Exception as e:
                print(f"[AngelOne] Login attempt {attempt+1} exception: {e}")
            time.sleep(2.0 * (attempt + 1))

        raise RuntimeError(
            "[AngelOne] Login FAILED after all retries.\n"
            "Check your API_KEY, CLIENT_ID, PASSWORD, TOTP_KEY.")

    def _ensure_logged_in(self):
        with self._lock:
            age = time.time() - self._login_ts
            if age > self.cfg["AO_TOKEN_TTL"]:
                print("[AngelOne] JWT expired — refreshing login...")
                self._login()

    def _base_headers(self) -> dict:
        return {
            "X-PrivateKey":    self.api_key,
            "X-UserType":      "USER",
            "X-SourceID":      "WEB",
            "X-ClientLocalIP": "127.0.0.1",
            "X-ClientPublicIP":"127.0.0.1",
            "X-MACAddress":    "00:00:00:00:00:00",
            "Accept":          "application/json",
            "Accept-Language": "en-US",
        }

    def _auth_headers(self) -> dict:
        h = self._base_headers()
        h["Authorization"] = f"Bearer {self._jwt}"
        return h

    # ── LTP Quote ────────────────────────────────────────────────────────────
    def _get_ltp(self, exchange: str, tokens: List[str]) -> Dict[str, float]:
        """
        Fetch Last Traded Price for a list of tokens on a given exchange.
        Returns {token: ltp_price}.
        Batches up to 50 tokens per request (AngelOne limit).
        """
        self._ensure_logged_in()
        result: Dict[str, float] = {}
        BATCH = 50

        for i in range(0, len(tokens), BATCH):
            batch = tokens[i: i + BATCH]
            payload = {
                "mode": "LTP",
                "exchangeTokens": {exchange: batch},
            }
            for attempt in range(self.cfg["AO_RETRY"] + 1):
                try:
                    r = requests.post(
                        f"{self.BASE_URL}/rest/secure/angelbroking/market/v1/quote/",
                        json=payload,
                        headers=self._auth_headers(),
                        timeout=self.cfg["AO_TIMEOUT"],
                    )
                    data = r.json()
                    if data.get("status") and data.get("data"):
                        fetched = data["data"].get("fetched", [])
                        for item in fetched:
                            tok   = str(item.get("symbolToken", ""))
                            price = item.get("ltp", 0.0)
                            if tok and price:
                                result[tok] = float(price)
                        break
                    else:
                        # Token might be expired
                        if "invalid" in str(data.get("message","")).lower():
                            with self._lock:
                                self._login_ts = 0
                            self._ensure_logged_in()
                except Exception:
                    pass
                time.sleep(1.0)
            time.sleep(self.AO_DELAY)

        return result

    # ── Public interface ──────────────────────────────────────────────────────
    def get_quote(self, symbol: str) -> Optional[Quote]:
        info = self._instrument_map.get(symbol)
        if not info:
            return None   # symbol not in master — likely no F&O contracts in range

        nse_token = info["nse_token"]
        nfo_list  = info["nfo_futures"]   # sorted near-month first

        # Fetch spot
        spot: Optional[float] = None
        if nse_token:
            prices = self._get_ltp("NSE", [nse_token])
            spot   = prices.get(nse_token)

        if not spot or spot <= 0:
            return None

        # Fetch near-month futures
        nfo_token   = nfo_list[0]["token"]
        expiry_str  = nfo_list[0]["expiry"]
        lot         = nfo_list[0]["lot"]

        prices = self._get_ltp("NFO", [nfo_token])
        fut    = prices.get(nfo_token)

        if not fut or fut <= 0:
            return None

        # Days to expiry (IST-aware)
        try:
            exp_date = datetime.strptime(expiry_str, "%d-%b-%Y").date()
            days     = max(1, (exp_date - now_ist().date()).days)
        except Exception:
            return None

        return Quote(
            symbol=symbol, spot=spot, futures=fut,
            lot_size=lot, expiry=expiry_str,
            days=days, source="AngelOne",
        )

    def name(self) -> str:
        return "AngelOne Smart API"


# ─────────────────────────────────────────────────────────────────────────────
# PROVIDER 2 — MOCK  (offline, great for testing without credentials)
# ─────────────────────────────────────────────────────────────────────────────
class MockProvider:
    """
    Generates realistic simulated quotes.
    • No internet required, no credentials needed.
    • Bypasses market-hours check automatically.
    • 8% chance of a 'fat spread' to simulate real arbitrage windows.
    """
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self._expiry, self._days = self._calc_expiry()

    def _calc_expiry(self) -> Tuple[str, int]:
        now = now_ist()
        m, y = now.month, now.year
        last_day = (datetime(y, m % 12 + 1, 1) - timedelta(days=1) if m < 12
                    else datetime(y + 1, 1, 1) - timedelta(days=1))
        d = last_day
        while d.weekday() != 3:   # last Thursday
            d -= timedelta(days=1)
        days = max(5, (d.date() - now.date()).days)
        if days < self.cfg["MIN_DAYS_TO_EXPIRY"]:
            d += timedelta(days=30)
            while d.weekday() != 3:
                d += timedelta(days=1)
            days = (d.date() - now.date()).days
        return d.strftime("%d-%b-%Y"), days

    def _gauss(self, sigma: float) -> float:
        u, v = random.random(), random.random()
        while u == 0: u = random.random()
        while v == 0: v = random.random()
        return sigma * math.sqrt(-2 * math.log(u)) * math.cos(2 * math.pi * v)

    def get_quote(self, symbol: str) -> Optional[Quote]:
        base = MOCK_SPOT.get(symbol, random.uniform(200, 8000))
        spot = round(max(10, base + self._gauss(base * 0.005)), 2)
        r    = random.uniform(0.07, 0.16)
        T    = self._days / 365
        fut  = round(spot * math.exp(r * T) + self._gauss(spot * 0.002), 2)
        if random.random() < 0.08:
            fut = round(fut * (1 + random.uniform(0.005, 0.025)), 2)
        if fut <= spot:
            return None
        lot = NSE_LOT_SIZES.get(symbol, DEFAULT_LOT)
        return Quote(symbol=symbol, spot=spot, futures=fut,
                     lot_size=lot, expiry=self._expiry,
                     days=self._days, source="Mock")

    def name(self) -> str:
        return "Mock (built-in offline data)"


# ─────────────────────────────────────────────────────────────────────────────
# PROVIDER FACTORY
# ─────────────────────────────────────────────────────────────────────────────
def build_provider(cfg: dict, provider_name: str,
                   api_key: str, client_id: str,
                   password: str, totp_key: str):
    label = provider_name.lower()

    if label == "mock":
        return MockProvider(cfg)

    if label in ("angelone", "auto", ""):
        # Validate credentials
        missing = [k for k, v in [("api_key",   api_key),
                                   ("client_id", client_id),
                                   ("password",  password)]
                   if not v.strip()]
        if missing:
            print(f"\n[ERROR] AngelOne credentials missing: {missing}")
            print("  Fill in ANGELONE_CREDS at the top of this file,")
            print("  OR pass them via --api-key / --client-id / --password\n")
            print("[FALLBACK] Switching to Mock provider for this session.")
            return MockProvider(cfg)
        if not REQUESTS_OK:
            print("[WARN] requests not installed → falling back to Mock")
            return MockProvider(cfg)
        return AngelOneProvider(cfg, api_key, client_id, password, totp_key)

    print(f"[WARN] Unknown provider '{provider_name}' → using Mock")
    return MockProvider(cfg)


# ─────────────────────────────────────────────────────────────────────────────
# SCANNER ENGINE
# ─────────────────────────────────────────────────────────────────────────────
class Scanner:
    def __init__(self, provider, cfg: dict):
        self.provider  = provider
        self.cfg       = cfg
        self.cost      = CostCalc(cfg)
        self._results: List[Opportunity] = []
        self._lock     = threading.Lock()

    def _classify(self, ann: float) -> str:
        if ann >= 18: return "STRONG BUY"
        if ann >= 12: return "BUY"
        if ann >= 8:  return "WATCH"
        return "SKIP"

    def scan_symbol(self, symbol: str) -> Optional[Opportunity]:
        q = self.provider.get_quote(symbol)
        if not q or q.spot <= 0 or q.futures <= 0:
            return None
        if not (self.cfg["MIN_DAYS_TO_EXPIRY"] <= q.days
                                                <= self.cfg["MAX_DAYS_TO_EXPIRY"]):
            return None

        raw = (q.futures - q.spot) / q.spot * 100
        if not (self.cfg["MIN_SPREAD_PCT"] <= raw <= self.cfg["MAX_SPREAD_PCT"]):
            return None

        cost   = self.cost.total_cost_pct(q.spot, q.futures, q.lot_size)
        net    = self.cost.net_spread(raw, cost)
        ann    = self.cost.annualized(net, q.days)
        if ann < self.cfg["MIN_ANNUALIZED_PCT"]:
            return None

        return Opportunity(
            symbol=q.symbol, spot=q.spot, futures=q.futures,
            raw_spread_pct=round(raw, 3),
            cost_pct=round(cost, 3),
            net_spread_pct=round(net, 3),
            ann_pct=round(ann, 1),
            days=q.days,
            lot_size=q.lot_size,
            margin_rs=self.cost.margin_required(q.futures, q.lot_size),
            profit_per_lot=self.cost.max_profit_per_lot(q.spot, net, q.lot_size),
            expiry=q.expiry,
            timestamp=now_ist().strftime("%H:%M:%S IST"),
            signal=self._classify(ann),
            source=q.source,
        )

    def scan_all(self) -> List[Opportunity]:
        results = []
        for sym in FNO_UNIVERSE:
            opp = self.scan_symbol(sym)
            if opp:
                results.append(opp)
        results.sort(key=lambda o: o.ann_pct, reverse=True)
        for i, o in enumerate(results):
            o.rank = i + 1
        with self._lock:
            self._results = results
        return results

    def top_n(self, n: int) -> List[Opportunity]:
        with self._lock:
            return self._results[:n]


# ─────────────────────────────────────────────────────────────────────────────
# PAPER TRADING ENGINE
# ─────────────────────────────────────────────────────────────────────────────
class PaperTrader:
    def __init__(self, cfg: dict):
        self.cfg        = cfg
        self.cost       = CostCalc(cfg)
        self.portfolio  = Portfolio(capital=cfg["PAPER_CAPITAL_RS"])
        self._pid       = 0
        self._switch_ts: Dict[str, float] = {}
        self._day_reset = now_ist().date()

    def _new_pid(self) -> str:
        self._pid += 1
        return f"POS-{self._pid:04d}"

    def _check_risk(self) -> Tuple[bool, str]:
        today = now_ist().date()
        if today != self._day_reset:
            self.portfolio.daily_pnl = 0.0
            self._day_reset = today
        p = self.portfolio
        if p.daily_pnl < -self.cfg["MAX_DAILY_LOSS_RS"]:
            return False, "Daily loss limit hit"
        if len(p.positions) >= self.cfg["MAX_OPEN_POSITIONS"]:
            return False, "Max open positions reached"
        if p.cash < self.cfg["MAX_POSITION_SIZE_RS"] * 0.4:
            return False, "Low cash"
        return True, ""

    def try_enter(self, opp: Opportunity) -> Optional[Position]:
        ok, _ = self._check_risk()
        if not ok or opp.symbol in self.portfolio.positions:
            return None
        cap   = min(self.cfg["MAX_POSITION_SIZE_RS"], self.portfolio.cash)
        lots  = max(1, int(cap / (opp.spot * opp.lot_size)))
        total = (opp.spot * opp.lot_size * lots
                 + self.cost.margin_required(opp.futures, opp.lot_size, lots))
        if total > self.portfolio.cash:
            lots  = max(1, lots - 1)
            total = (opp.spot * opp.lot_size * lots
                     + self.cost.margin_required(opp.futures, opp.lot_size, lots))
        if total > self.portfolio.cash:
            return None
        pos = Position(
            pid=self._new_pid(), symbol=opp.symbol,
            entry_spot=opp.spot, entry_fut=opp.futures,
            entry_spread_pct=opp.raw_spread_pct,
            net_spread_pct=opp.net_spread_pct,
            lots=lots, lot_size=opp.lot_size, capital_rs=total,
            entry_time=now_ist().strftime("%Y-%m-%d %H:%M IST"),
            expiry=opp.expiry, days_at_entry=opp.days,
        )
        self.portfolio.positions[opp.symbol] = pos
        self.portfolio.cash     -= total
        self.portfolio.deployed += total
        self.portfolio.trades   += 1
        return pos

    def update(self, opps: List[Opportunity]):
        omap = {o.symbol: o for o in opps}
        for sym in list(self.portfolio.positions):
            pos = self.portfolio.positions[sym]
            if pos.status != "OPEN":
                continue
            try:
                exp_date  = datetime.strptime(pos.expiry, "%d-%b-%Y").date()
                days_left = (exp_date - now_ist().date()).days
            except Exception:
                days_left = 5
            if days_left <= 0:
                self._close(pos, "EXPIRY")
                continue
            cur = omap.get(sym)
            if cur:
                spread_gain = pos.entry_spread_pct - cur.raw_spread_pct
                pos.unrealized_pnl = round(
                    spread_gain / 100 * pos.entry_spot * pos.lot_size * pos.lots, 2)
                if cur.raw_spread_pct < self.cfg["STOP_LOSS_SPREAD_PCT"]:
                    self._close(pos, "STOP_LOSS",
                                exit_spot=cur.spot, exit_fut=cur.futures)
                    continue
                self._try_switch(pos, opps)

    def _try_switch(self, pos: Position, opps: List[Opportunity]):
        now_t = time.time()
        last  = self._switch_ts.get(pos.symbol, 0)
        if now_t - last < self.cfg["SWITCH_COOLDOWN_SEC"]:
            return
        better = next(
            (o for o in opps
             if o.symbol != pos.symbol
             and o.symbol not in self.portfolio.positions
             and o.net_spread_pct > pos.net_spread_pct
                                    + self.cfg["SWITCH_THRESHOLD_PCT"]),
            None)
        if better:
            self._close(pos, f"SWITCH→{better.symbol}")
            self._switch_ts[pos.symbol] = now_t
            self.try_enter(better)

    def _close(self, pos: Position, reason: str = "MANUAL",
               exit_spot: float = None, exit_fut: float = None):
        if reason == "EXPIRY":
            es = pos.entry_spot * (1 + random.gauss(0, 0.008))
            ef = es
        else:
            es = exit_spot or pos.entry_spot
            ef = exit_fut  or pos.entry_fut
        spot_pnl = (es - pos.entry_spot) * pos.lot_size * pos.lots
        fut_pnl  = (pos.entry_fut - ef)  * pos.lot_size * pos.lots
        gross    = spot_pnl + fut_pnl
        cost_pct = self.cost.total_cost_pct(
            pos.entry_spot, pos.entry_fut, pos.lot_size, pos.lots)
        cost_rs  = pos.entry_spot * pos.lot_size * pos.lots * cost_pct / 100
        net_pnl  = round(gross - cost_rs, 2)
        pos.exit_spot    = round(es, 2)
        pos.exit_fut     = round(ef, 2)
        pos.exit_time    = now_ist().strftime("%Y-%m-%d %H:%M IST")
        pos.exit_reason  = reason
        pos.realized_pnl = net_pnl
        pos.status       = "CLOSED"
        self.portfolio.cash      += pos.capital_rs + net_pnl
        self.portfolio.deployed  -= pos.capital_rs
        self.portfolio.total_pnl += net_pnl
        self.portfolio.daily_pnl += net_pnl
        if net_pnl > 0:
            self.portfolio.wins += 1
        del self.portfolio.positions[pos.symbol]


# ─────────────────────────────────────────────────────────────────────────────
# CSV LOGGER
# ─────────────────────────────────────────────────────────────────────────────
class Logger:
    def __init__(self, cfg: dict):
        self.d = Path(cfg["LOG_DIR"])
        self.d.mkdir(exist_ok=True)
        self._init(self.d / cfg["SCAN_CSV"],
                   ["ts","rank","symbol","spot","futures","raw%","cost%","net%",
                    "ann%","days","lot","margin","profit_lot","expiry","signal","source"])
        self._init(self.d / cfg["TRADES_CSV"],
                   ["pid","symbol","entry_spot","entry_fut","spread%","net%",
                    "lots","lot_size","capital","entry_time","expiry",
                    "exit_spot","exit_fut","exit_time","reason","pnl"])
        self._init(self.d / cfg["PNL_CSV"],
                   ["ts","total_pnl","daily_pnl","cash","deployed",
                    "open_pos","trades","win_rate%"])
        self.scan_file   = self.d / cfg["SCAN_CSV"]
        self.trades_file = self.d / cfg["TRADES_CSV"]
        self.pnl_file    = self.d / cfg["PNL_CSV"]

    def _init(self, path: Path, header: list):
        if not path.exists():
            with open(path, "w", newline="") as f:
                csv.writer(f).writerow(header)

    def scan(self, opps: List[Opportunity]):
        with open(self.scan_file, "a", newline="") as f:
            w = csv.writer(f)
            for o in opps:
                w.writerow([o.timestamp, o.rank, o.symbol, o.spot, o.futures,
                             o.raw_spread_pct, o.cost_pct, o.net_spread_pct,
                             o.ann_pct, o.days, o.lot_size, o.margin_rs,
                             o.profit_per_lot, o.expiry, o.signal, o.source])

    def trade(self, pos: Position):
        with open(self.trades_file, "a", newline="") as f:
            csv.writer(f).writerow([
                pos.pid, pos.symbol, pos.entry_spot, pos.entry_fut,
                pos.entry_spread_pct, pos.net_spread_pct,
                pos.lots, pos.lot_size, pos.capital_rs,
                pos.entry_time, pos.expiry,
                pos.exit_spot or "", pos.exit_fut or "", pos.exit_time or "",
                pos.exit_reason, pos.realized_pnl])

    def pnl(self, p: Portfolio):
        wr = round(p.wins / p.trades * 100, 1) if p.trades > 0 else 0
        with open(self.pnl_file, "a", newline="") as f:
            csv.writer(f).writerow([
                now_ist().strftime("%Y-%m-%d %H:%M:%S IST"),
                round(p.total_pnl, 2), round(p.daily_pnl, 2),
                round(p.cash, 2), round(p.deployed, 2),
                len(p.positions), p.trades, wr])


# ─────────────────────────────────────────────────────────────────────────────
# TERMINAL DASHBOARD  (rich or plain fallback)
# ─────────────────────────────────────────────────────────────────────────────
class Dashboard:
    def __init__(self, cfg: dict, provider_name: str):
        self.cfg   = cfg
        self.pname = provider_name
        self.con   = Console() if RICH_OK else None

    @staticmethod
    def _sig_style(sig: str) -> str:
        return {"STRONG BUY": "bold green", "BUY": "green",
                "WATCH": "yellow", "SKIP": "dim"}.get(sig, "white")

    @staticmethod
    def _pnl_style(v: float) -> str:
        return "bold green" if v > 0 else "bold red" if v < 0 else "white"

    def render(self, opps: List[Opportunity], port: Portfolio,
               scan_n: int, mkt_status: str):
        if not RICH_OK:
            self._plain(opps, port, scan_n, mkt_status)
            return None

        t        = now_ist()
        is_open  = "OPEN" in mkt_status or "BYPASS" in mkt_status
        mkt_col  = "bold green" if is_open else "bold red"

        header = Panel(
            Text.from_markup(
                f"[bold cyan]⚡ CASH & CARRY ARBITRAGE SCANNER[/bold cyan]   "
                f"[dim]IST {t.strftime('%Y-%m-%d %H:%M:%S')}[/dim]   "
                f"[{mkt_col}]● MARKET: {mkt_status}[/{mkt_col}]   "
                f"[dim]Scan #{scan_n} | {self.pname}[/dim]"
            ),
            border_style="bright_blue",
        )

        # Opportunities table
        tbl = Table(box=box.SIMPLE_HEAVY, show_header=True,
                    header_style="bold bright_cyan", expand=True)
        for col, w, j in [
            ("#",        3,  "right"), ("Symbol",   14, "left"),
            ("Spot ₹",  10,  "right"), ("Fut ₹",   10, "right"),
            ("Raw %",    7,  "right"), ("Cost %",   7, "right"),
            ("Net %",    7,  "right"), ("Ann %",    8, "right"),
            ("Days",     5,  "right"), ("Lot",      6, "right"),
            ("Margin ₹",12,  "right"), ("Profit/Lot",10,"right"),
            ("Expiry",  12,  "left"),  ("Signal",  11, "left"),
            ("Source",  22,  "left"),
        ]:
            tbl.add_column(col, width=w, justify=j)

        for o in opps[:self.cfg["TOP_N_DISPLAY"]]:
            ss = self._sig_style(o.signal)
            tbl.add_row(
                str(o.rank),
                f"[bold]{o.symbol}[/bold]",
                f"{o.spot:,.2f}", f"{o.futures:,.2f}",
                f"[yellow]{o.raw_spread_pct:.2f}[/yellow]",
                f"[dim]{o.cost_pct:.3f}[/dim]",
                f"[cyan]{o.net_spread_pct:.2f}[/cyan]",
                f"[{ss}]{o.ann_pct:.1f}[/{ss}]",
                str(o.days), str(o.lot_size),
                f"₹{o.margin_rs:,.0f}", f"₹{o.profit_per_lot:,.0f}",
                o.expiry,
                f"[{ss}]{o.signal}[/{ss}]",
                f"[dim]{o.source}[/dim]",
            )
        if not opps:
            note = " (market closed)" if not is_open else ""
            tbl.add_row(*[""] * 14,
                        f"[dim]No opportunities found{note}[/dim]")

        # Portfolio panel
        p  = port
        wr = round(p.wins / p.trades * 100, 1) if p.trades > 0 else 0.0
        port_panel = Panel(
            Text.from_markup(
                f"[bold]Capital:[/bold] ₹{p.capital:,.0f}  "
                f"[bold]Cash:[/bold] ₹{p.cash:,.0f}  "
                f"[bold]Deployed:[/bold] ₹{p.deployed:,.0f}  "
                f"[bold]Total P&L:[/bold] [{self._pnl_style(p.total_pnl)}]"
                f"₹{p.total_pnl:+,.2f}[/{self._pnl_style(p.total_pnl)}]  "
                f"[bold]Daily P&L:[/bold] [{self._pnl_style(p.daily_pnl)}]"
                f"₹{p.daily_pnl:+,.2f}[/{self._pnl_style(p.daily_pnl)}]  "
                f"[bold]Trades:[/bold] {p.trades}  "
                f"[bold]Win%:[/bold] {wr:.1f}%  "
                f"[bold]Open Pos:[/bold] {len(p.positions)}"
            ),
            title="[bold]Paper Portfolio[/bold]",
            border_style="green",
        )

        # Open positions
        pos_lines = []
        for sym, pos in p.positions.items():
            us = self._pnl_style(pos.unrealized_pnl)
            pos_lines.append(
                f"  [{us}]▶[/{us}] {sym}  spread={pos.entry_spread_pct:.2f}%  "
                f"lots={pos.lots}  cap=₹{pos.capital_rs:,.0f}  "
                f"uPnL=[{us}]₹{pos.unrealized_pnl:+,.2f}[/{us}]  "
                f"exp={pos.expiry}"
            )
        pos_panel = Panel(
            Text.from_markup(
                "\n".join(pos_lines) if pos_lines else "  [dim]No open positions[/dim]"
            ),
            title="[bold]Open Positions[/bold]",
            border_style="yellow",
        )

        return Group(header, tbl, port_panel, pos_panel)

    def _plain(self, opps: List[Opportunity], port: Portfolio,
               scan_n: int, mkt_status: str):
        t = now_ist()
        print(f"\n{'='*80}")
        print(f"  CASH & CARRY SCANNER | Scan #{scan_n} | IST {t.strftime('%H:%M:%S')}")
        print(f"  Market: {mkt_status} | Provider: {self.pname}")
        print(f"{'='*80}")
        print(f"{'#':>3} {'Symbol':<14} {'Spot':>10} {'Fut':>10} "
              f"{'Raw%':>6} {'Net%':>6} {'Ann%':>7} {'Days':>5} {'Signal':<12}")
        print("-" * 80)
        for o in opps[:self.cfg["TOP_N_DISPLAY"]]:
            print(f"{o.rank:>3} {o.symbol:<14} {o.spot:>10,.2f} {o.futures:>10,.2f} "
                  f"{o.raw_spread_pct:>6.2f} {o.net_spread_pct:>6.2f} "
                  f"{o.ann_pct:>7.1f} {o.days:>5} {o.signal:<12}")
        if not opps:
            print("  [No opportunities found]")
        p = port
        print(f"\n  Capital: ₹{p.capital:,.0f}  Cash: ₹{p.cash:,.0f}  "
              f"P&L: ₹{p.total_pnl:+,.2f}  Trades: {p.trades}")
        print(f"{'='*80}\n")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────────────────────────────────────
def run(cfg: dict, provider, once: bool = False, min_ann: float = 0.0):
    cfg["MIN_ANNUALIZED_PCT"] = min_ann
    scanner    = Scanner(provider, cfg)
    trader     = PaperTrader(cfg)
    logger     = Logger(cfg)
    dash       = Dashboard(cfg, provider.name())
    scan_n     = 0
    last_opps: List[Opportunity] = []
    is_mock    = "Mock" in provider.name()

    print(f"\n[BOOT] Provider  : {provider.name()}")
    print(f"[BOOT] IST Time  : {now_ist().strftime('%Y-%m-%d %H:%M:%S')}")
    _, mstatus = is_market_open(cfg)
    print(f"[BOOT] Market    : {mstatus}")
    print(f"[BOOT] Universe  : {len(FNO_UNIVERSE)} F&O stocks")
    print(f"[BOOT] Scan every: {cfg['SCAN_INTERVAL_SECONDS']}s\n")

    def _one_scan():
        nonlocal scan_n, last_opps
        scan_n += 1
        mopen, mstatus = is_market_open(cfg)

        if mopen or is_mock or cfg.get("SKIP_MARKET_HOURS_CHECK", False):
            opps = scanner.scan_all()
            if opps:
                last_opps = opps
                logger.scan(opps)
                trader.update(opps)
                if opps[0].signal in ("STRONG BUY", "BUY"):
                    new_pos = trader.try_enter(opps[0])
                    if new_pos:
                        logger.trade(new_pos)
                logger.pnl(trader.portfolio)

        return dash.render(last_opps, trader.portfolio, scan_n, mstatus), mstatus

    if RICH_OK:
        with Live(console=dash.con, refresh_per_second=1, screen=True) as live:
            while True:
                renderable, _ = _one_scan()
                if renderable:
                    live.update(renderable)
                if once:
                    break
                time.sleep(cfg["SCAN_INTERVAL_SECONDS"])
    else:
        while True:
            _one_scan()
            if once:
                break
            time.sleep(cfg["SCAN_INTERVAL_SECONDS"])


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(
        description="Cash & Carry Arbitrage Scanner — AngelOne Smart API")

    ap.add_argument("--provider",
                    choices=["angelone", "mock", "auto"],
                    default="auto",
                    help="Data provider (default: auto → AngelOne if creds present, else Mock)")
    ap.add_argument("--api-key",   default="",
                    help="AngelOne API key (overrides ANGELONE_CREDS in code)")
    ap.add_argument("--client-id", default="",
                    help="AngelOne client code / login ID")
    ap.add_argument("--password",  default="",
                    help="AngelOne MPIN / password")
    ap.add_argument("--totp-key",  default="",
                    help="TOTP secret (from AngelOne 2FA setup). Leave blank if 2FA disabled.")
    ap.add_argument("--once",       action="store_true",
                    help="Single scan then exit")
    ap.add_argument("--min-ann",    type=float, default=0.0,
                    help="Min annualized return %% to display (default: 0)")
    ap.add_argument("--no-market-check", action="store_true",
                    help="Bypass NSE market-hours check (scan any time, great for testing)")
    ap.add_argument("--top",        type=int, default=CONFIG["TOP_N_DISPLAY"],
                    help=f"Rows to display (default: {CONFIG['TOP_N_DISPLAY']})")
    ap.add_argument("--interval",   type=int, default=CONFIG["SCAN_INTERVAL_SECONDS"],
                    help=f"Seconds between scans (default: {CONFIG['SCAN_INTERVAL_SECONDS']})")

    # parse_known_args: ignores Jupyter/Colab's extra -f kernel.json flag
    args, _unknown = ap.parse_known_args()

    cfg = dict(CONFIG)
    cfg["SKIP_MARKET_HOURS_CHECK"] = args.no_market_check
    cfg["TOP_N_DISPLAY"]           = args.top
    cfg["SCAN_INTERVAL_SECONDS"]   = args.interval

    # Merge credentials: CLI args override in-code ANGELONE_CREDS
    api_key   = args.api_key   or ANGELONE_CREDS["API_KEY"]
    client_id = args.client_id or ANGELONE_CREDS["CLIENT_ID"]
    password  = args.password  or ANGELONE_CREDS["PASSWORD"]
    totp_key  = args.totp_key  or ANGELONE_CREDS["TOTP_KEY"]

    provider = build_provider(cfg, args.provider,
                              api_key, client_id, password, totp_key)

    try:
        run(cfg, provider, once=args.once, min_ann=args.min_ann)
    except KeyboardInterrupt:
        print("\n\n[STOP] Scanner stopped. Goodbye!")
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback; traceback.print_exc()


if __name__ == "__main__":
    main()
