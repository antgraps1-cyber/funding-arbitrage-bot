#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║        CASH & CARRY ARBITRAGE SCANNER  —  INDIAN F&O MARKETS               ║
║        100% FREE  |  NO BROKER API KEY  |  No paid subscription             ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  DATA SOURCES (all free, no login needed):                                  ║
║  1. NSEProvider    — NSE India public REST API (same data as website)       ║
║  2. YFinanceProvider — Yahoo Finance via yfinance library                   ║
║  3. HybridProvider  — yfinance spot + NSE futures (most reliable combo)     ║
║  4. MockProvider    — Built-in fake data (always works, no internet needed) ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  INSTALL (one-time):                                                        ║
║    pip install yfinance nsepython requests pandas rich                      ║
║                                                                             ║
║  RUN:                                                                       ║
║    python arb_scanner.py              # auto-detect best provider           ║
║    python arb_scanner.py --provider nse        # NSE public API             ║
║    python arb_scanner.py --provider yfinance   # Yahoo Finance              ║
║    python arb_scanner.py --provider hybrid     # yfinance + NSE             ║
║    python arb_scanner.py --provider mock       # offline test               ║
║    python arb_scanner.py --once                # single scan and exit       ║
║    python arb_scanner.py --min-ann 12          # only show >12% annualized  ║
╚══════════════════════════════════════════════════════════════════════════════╝

STRATEGY: Cash & Carry Arbitrage (Spot–Future Arbitrage)
  BUY spot stock  +  SELL near-month futures
  Lock-in spread = (Futures - Spot) / Spot * 100%
  Hold to expiry → prices converge → profit = net spread after costs
  Early exit if a BETTER opportunity appears (switching logic)

COSTS MODEL (accurate for Indian markets):
  STT sell delivery:   0.100%   STT sell futures:  0.010%
  Exchange charges:    0.00345% SEBI fee:          0.0001%
  Stamp duty:          0.015%   Brokerage:         Rs 20/order x4
  GST on brokerage:    18%      Slippage (total):  0.30% round-trip
  ─────────────────────────────────────────────────────
  Typical total round-trip: ~0.55–0.65%
"""

# ─────────────────────────────────────────────────────────────────────────────
# STDLIB  (zero extra installs for these)
# ─────────────────────────────────────────────────────────────────────────────
import argparse, csv, json, math, os, random, re, sys, time, threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ─────────────────────────────────────────────────────────────────────────────
# OPTIONAL DEPS  (graceful fallback if not installed)
# ─────────────────────────────────────────────────────────────────────────────
try:
    import requests
    REQUESTS_OK = True
except ImportError:
    REQUESTS_OK = False

try:
    import yfinance as yf
    YFINANCE_OK = True
except ImportError:
    YFINANCE_OK = False

try:
    import nsepython as nsepy
    NSEPYTHON_OK = True
except ImportError:
    NSEPYTHON_OK = False

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
    print("[TIP] Run  pip install rich  for a colour terminal dashboard.\n")


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION  — edit these values to tune the bot
# ─────────────────────────────────────────────────────────────────────────────
CONFIG = {
    # ── Scanner ──────────────────────────────────────────────────────────────
    "SCAN_INTERVAL_SECONDS":   60,    # seconds between full scans
    "TOP_N_DISPLAY":           20,    # rows shown in the table
    "MIN_SPREAD_PCT":          4.0,   # entry trigger: only trade if raw spread > 4%
    "MAX_SPREAD_PCT":          15.0,  # ignore spreads larger (likely bad data)
    "MIN_DAYS_TO_EXPIRY":      3,     # skip expiries within 3 days
    "MAX_DAYS_TO_EXPIRY":      35,    # near-month only

    # ── Cost model ───────────────────────────────────────────────────────────
    "BROKERAGE_PER_ORDER_RS":  20,    # flat brokerage per order (Zerodha / Groww etc.)
    "STT_SELL_DELIVERY_PCT":   0.1,   # Securities Transaction Tax — delivery sell side
    "STT_SELL_FUTURES_PCT":    0.01,  # STT — futures sell side
    "EXCHANGE_TXN_PCT":        0.00345,
    "SEBI_FEE_PCT":            0.0001,
    "STAMP_DUTY_PCT":          0.015, # on buy side
    "GST_PCT":                 18,    # on brokerage
    # Total round-trip slippage across all 4 legs combined (% of spot value)
    # Covers: buy spot + sell futures (entry) and sell spot + buy futures (exit)
    "OVERALL_SLIPPAGE_PCT":    0.3,   # 0.3% total — realistic for Indian F&O retail

    # ── Paper trading ────────────────────────────────────────────────────────
    "PAPER_CAPITAL_RS":        1_500_000, # ₹15 lakh virtual capital
    "MAX_POSITION_SIZE_RS":    300_000,   # max capital per trade (₹3 lakh)
    "MAX_OPEN_POSITIONS":      5,
    "FUTURES_MARGIN_PCT":      15.0,     # approx SPAN margin % of contract value

    # ── Risk management ──────────────────────────────────────────────────────
    "STOP_LOSS_SPREAD_PCT":    -0.5,     # exit if spread turns negative by 0.5%
    "MAX_DAILY_LOSS_RS":       5_000,
    "MIN_ANNUALIZED_PCT":      0.0,      # disabled — entry driven by MIN_SPREAD_PCT 4% rule

    # ── Switching logic ──────────────────────────────────────────────────────
    "SWITCH_THRESHOLD_PCT":    0.50,     # switch only if new net spread > current + 0.5%
    "SWITCH_COOLDOWN_SEC":     300,      # min seconds between switches

    # ── Logging ──────────────────────────────────────────────────────────────
    "LOG_DIR":       "./arb_logs",
    "SCAN_CSV":      "scans.csv",
    "TRADES_CSV":    "trades.csv",
    "PNL_CSV":       "pnl.csv",

    # ── NSE session ──────────────────────────────────────────────────────────
    "NSE_TIMEOUT":             12,       # seconds per HTTP request
    "NSE_RETRY":               2,        # retries on network error
    "NSE_SESSION_TTL":         480,      # re-init session every 8 minutes
    "NSE_DELAY_SEC":           0.25,     # polite delay between NSE requests
}

# ─────────────────────────────────────────────────────────────────────────────
# F&O STOCK UNIVERSE — NSE F&O eligible stocks (150+)
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
    "AMARARAJA","BALKRISIND","MRF","JKTYRE","MOTHERSUMI","BHARATFORG",
    "ABB","LALPATHLAB","METROPOLIS","ALKEM","TORNTPHARM","GRANULES","BIOCON",
    "LAURUSLABS","IPCA","NATCOPHARMA","ZYDUSLIFE","GLAND","ABBOTINDIA",
    "SHRIRAMFIN","LTFH","MOTILALOFS","ANGELONE","BSE","MCX","CDSL",
    "GNFC","NOCIL","RATNAMANI","SONACOMS","KAYNES","SHOPERSTOP","WHIRLPOOL",
    "PAGEIND","MCDOWELL-N","UBL","EMAMILTD","CROMPTON","BLUESTARCO","ATGL",
    "NYKAA","PAYTM","POLICYBZR","EASEMYTRIP","INDIAMART","JUSTDIAL","AFFLE",
]
FNO_UNIVERSE = list(dict.fromkeys(FNO_UNIVERSE))  # deduplicate, preserve order

# Official NSE lot sizes (Jan 2025 revision — update quarterly from NSE)
NSE_LOT_SIZES: Dict[str, int] = {
    "RELIANCE":250, "TCS":150, "INFY":300, "HDFCBANK":550, "ICICIBANK":1375,
    "HINDUNILVR":300, "KOTAKBANK":400, "SBIN":1500, "BAJFINANCE":125,
    "BHARTIARTL":1851, "ITC":3200, "ASIANPAINT":300, "AXISBANK":1200,
    "MARUTI":100, "TITAN":375, "NESTLEIND":50, "ULTRACEMCO":100,
    "TECHM":600, "HCLTECH":700, "WIPRO":1500, "SUNPHARMA":700,
    "DIVISLAB":200, "DRREDDY":125, "CIPLA":650, "TATAMOTORS":2850,
    "M&M":700, "BAJAJFINSV":500, "ONGC":3850, "POWERGRID":4700,
    "NTPC":5750, "COALINDIA":4200, "BPCL":1800, "IOC":4750, "GAIL":3850,
    "ADANIPORTS":1250, "ADANIENT":500, "ADANIGREEN":500, "TATAPOWER":6750,
    "DMART":187, "BAJAJ-AUTO":250, "HEROMOTOCO":300, "EICHERMOT":200,
    "BRITANNIA":200, "HAVELLS":1000, "DABUR":1250, "MARICO":1400,
    "GODREJCP":500, "COLPAL":500, "TATACONSUM":1200, "PIDILITIND":500,
    "BERGERPAINTS":1100, "SIEMENS":275, "ABB":250, "VOLTAS":1000,
    "POLYCAB":400, "CUMMINSIND":600, "BOSCHLTD":50, "TATASTEEL":3500,
    "JSWSTEEL":1350, "SAIL":7000, "HINDALCO":2150, "VEDL":3000,
    "NATIONALUM":8500, "HINDZINC":2700, "CONCOR":2000, "IRCTC":2400,
    "INDIGO":600, "GMRINFRA":11250, "NHPC":15000, "SJVN":12500,
    "RECLTD":3000, "PFC":3500, "IRFC":7000, "HDFCLIFE":1100,
    "SBILIFE":750, "ICICIPRULI":1500, "LICI":700, "CHOLAFIN":500,
    "MUTHOOTFIN":750, "AUBANK":1000, "FEDERALBNK":10000, "IDFCFIRSTB":10000,
    "RBLBANK":5000, "BANDHANBNK":5000, "MANAPPURAM":4000, "INFOEDGE":300,
    "ZOMATO":3750, "NAUKRI":300, "MPHASIS":400, "PERSISTENT":250,
    "LTTS":200, "COFORGE":200, "TRENT":475, "PVRINOX":1000,
    "ZEEL":3000, "SUNTV":1400, "DEEPAKNITR":750, "AARTIIND":1300,
    "TATACHEMICALS":1000, "GUJGASLTD":1000, "IGL":2750, "MGL":550,
    "PETRONET":3000, "CHAMBLFERT":2600, "COROMANDEL":500, "UPL":1300,
    "APOLLOHOSP":300, "APOLLOTYRE":3500, "EXIDEIND":5400, "AMARARAJA":1100,
    "BALKRISIND":400, "MRF":10, "TATASTEEL":3500, "BHARATFORG":1200,
    "SHRIRAMFIN":500, "LTFH":8000, "MOTILALOFS":400, "ANGELONE":350,
    "BSE":400, "MCX":250, "CDSL":2500, "GNFC":2100, "BIOCON":2500,
    "LAURUSLABS":1800, "IPCA":500, "ZYDUSLIFE":700, "GLAND":350,
    "ALKEM":300, "TORNTPHARM":500, "METROPOLIS":400, "LALPATHLAB":300,
}
DEFAULT_LOT = 500

# Reference spot prices for mock mode
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
    symbol:     str
    spot:       float
    futures:    float
    lot_size:   int
    expiry:     str    # "29-May-2025"
    days:       int
    source:     str    # "NSE" | "YFinance" | "Hybrid" | "Mock"

@dataclass
class Opportunity:
    symbol:          str
    spot:            float
    futures:         float
    raw_spread_pct:  float
    cost_pct:        float
    net_spread_pct:  float
    ann_pct:         float
    days:            int
    lot_size:        int
    margin_rs:       float
    profit_per_lot:  float
    expiry:          str
    timestamp:       str
    signal:          str   # "STRONG BUY" | "BUY" | "WATCH" | "SKIP"
    source:          str
    rank:            int = 0

@dataclass
class Position:
    pid:                str
    symbol:             str
    entry_spot:         float
    entry_fut:          float
    entry_spread_pct:   float
    net_spread_pct:     float
    lots:               int
    lot_size:           int
    capital_rs:         float
    entry_time:         str
    expiry:             str
    days_at_entry:      int
    status:             str  = "OPEN"
    exit_spot:          Optional[float] = None
    exit_fut:           Optional[float] = None
    exit_time:          Optional[str]   = None
    exit_reason:        str  = ""
    realized_pnl:       float = 0.0
    unrealized_pnl:     float = 0.0

@dataclass
class Portfolio:
    capital:   float
    cash:      float     = 0.0
    deployed:  float     = 0.0
    total_pnl: float     = 0.0
    daily_pnl: float     = 0.0
    trades:    int       = 0
    wins:      int       = 0
    positions: dict      = field(default_factory=dict)
    scans:     int       = 0
    def __post_init__(self): self.cash = self.capital


# ─────────────────────────────────────────────────────────────────────────────
# COST CALCULATOR
# ─────────────────────────────────────────────────────────────────────────────
class CostCalc:
    def __init__(self, cfg: dict):
        self.cfg = cfg

    def total_cost_pct(self, spot: float, fut: float, lot: int, lots: int = 1) -> float:
        """Full round-trip cost as % of spot value."""
        sv = spot * lot * lots
        fv = fut  * lot * lots
        brok  = self.cfg["BROKERAGE_PER_ORDER_RS"] * 4
        gst   = brok * self.cfg["GST_PCT"] / 100
        stt_s = sv * self.cfg["STT_SELL_DELIVERY_PCT"] / 100
        stt_f = fv * self.cfg["STT_SELL_FUTURES_PCT"]  / 100
        exc   = (sv + fv) * 2 * self.cfg["EXCHANGE_TXN_PCT"] / 100
        sebi  = (sv + fv) * 2 * self.cfg["SEBI_FEE_PCT"]     / 100
        stamp = sv * self.cfg["STAMP_DUTY_PCT"] / 100
        # Slippage: OVERALL_SLIPPAGE_PCT is the TOTAL round-trip cost across all 4 legs
        # directly as % of spot value — no further multiplication needed
        slip  = sv * self.cfg["OVERALL_SLIPPAGE_PCT"] / 100
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
# PROVIDER 1 — NSE INDIA PUBLIC REST API  (no auth, same source as NSE website)
# ─────────────────────────────────────────────────────────────────────────────
class NSEProvider:
    """
    Uses NSE India's unauthenticated public JSON endpoints.
    Endpoints:
      Spot    : GET /api/quote-equity?symbol=RELIANCE
      Futures : GET /api/quote-derivative?symbol=RELIANCE
      Market  : GET /api/marketStatus
    The NSE server requires a browser-like session (cookies + headers).
    We prime the session by visiting the main page first.
    Session cookies expire ~8 min → auto-refresh built in.
    """
    BASE = "https://www.nseindia.com"
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept":           "application/json, text/plain, */*",
        "Accept-Language":  "en-US,en;q=0.9",
        "Accept-Encoding":  "gzip, deflate, br",
        "Referer":          "https://www.nseindia.com/",
        "Connection":       "keep-alive",
        "sec-fetch-dest":   "empty",
        "sec-fetch-mode":   "cors",
        "sec-fetch-site":   "same-origin",
    }

    def __init__(self, cfg: dict):
        if not REQUESTS_OK:
            raise RuntimeError("requests not installed. Run: pip install requests")
        self.cfg     = cfg
        self._s      = None
        self._init_ts = 0.0
        self._lock   = threading.Lock()

    def _init_session(self):
        """Visit main page and option-chain page to acquire NSE cookies."""
        s = requests.Session()
        s.headers.update(self.HEADERS)
        s.get(self.BASE + "/", timeout=self.cfg["NSE_TIMEOUT"])
        time.sleep(0.6)
        s.get(self.BASE + "/get-quotes/equity?symbol=SBIN",
              timeout=self.cfg["NSE_TIMEOUT"])
        time.sleep(0.4)
        self._s      = s
        self._init_ts = time.time()

    def _session(self) -> requests.Session:
        with self._lock:
            if (self._s is None or
                    time.time() - self._init_ts > self.cfg["NSE_SESSION_TTL"]):
                self._init_session()
        return self._s

    def _fetch(self, url: str) -> Optional[dict]:
        """GET with retry and session-refresh on 401/403."""
        for attempt in range(self.cfg["NSE_RETRY"] + 1):
            try:
                r = self._session().get(url, timeout=self.cfg["NSE_TIMEOUT"])
                if r.status_code in (401, 403):
                    self._init_ts = 0  # force re-init
                    time.sleep(1.5)
                    continue
                if r.status_code == 200:
                    return r.json()
            except Exception:
                pass
            time.sleep(0.8)
        return None

    def _expiry_days(self, expiry_str: str) -> int:
        """Days from today to given expiry string like '29-May-2025'."""
        try:
            dt = datetime.strptime(expiry_str, "%d-%b-%Y")
            return max(1, (dt - datetime.now()).days)
        except Exception:
            return 99

    def get_spot(self, symbol: str) -> Optional[float]:
        data = self._fetch(f"{self.BASE}/api/quote-equity?symbol={symbol}")
        if not data:
            return None
        return data.get("priceInfo", {}).get("lastPrice")

    def get_futures(self, symbol: str) -> Tuple[Optional[float], Optional[str], int, int]:
        """Returns (futures_price, expiry_str, days_to_expiry, lot_size)."""
        data = self._fetch(f"{self.BASE}/api/quote-derivative?symbol={symbol}")
        if not data:
            return None, None, 0, DEFAULT_LOT
        contracts = []
        for item in data.get("stocks", []):
            md = item.get("metadata", {})
            if "Stock Futures" not in md.get("instrumentType", ""):
                continue
            expiry = md.get("expiryDate", "")
            days   = self._expiry_days(expiry)
            if days < 1:
                continue
            price  = (md.get("lastPrice") or
                      item.get("marketDeptOrderBook", {})
                          .get("tradeInfo", {}).get("lastPrice"))
            if not price or price <= 0:
                continue
            lot = int(md.get("lotSize") or NSE_LOT_SIZES.get(symbol, DEFAULT_LOT))
            contracts.append({"price": float(price), "expiry": expiry,
                               "days": days, "lot": lot})
        if not contracts:
            return None, None, 0, DEFAULT_LOT
        # Sort by expiry → take near-month
        contracts.sort(key=lambda c: c["days"])
        best = contracts[0]
        return best["price"], best["expiry"], best["days"], best["lot"]

    def get_quote(self, symbol: str) -> Optional[Quote]:
        try:
            spot = self.get_spot(symbol)
            time.sleep(self.cfg["NSE_DELAY_SEC"])
            fut_price, expiry, days, lot = self.get_futures(symbol)
            time.sleep(self.cfg["NSE_DELAY_SEC"])
            if not spot or not fut_price or days < 1:
                return None
            return Quote(symbol=symbol, spot=spot, futures=fut_price,
                         lot_size=lot, expiry=expiry, days=days, source="NSE")
        except Exception:
            return None

    def name(self) -> str: return "NSE Public API"


# ─────────────────────────────────────────────────────────────────────────────
# PROVIDER 2 — YAHOO FINANCE via yfinance  (spot + theoretical futures)
# ─────────────────────────────────────────────────────────────────────────────
class YFinanceProvider:
    """
    Uses the yfinance library (Yahoo Finance) for spot prices.
    NSE stocks are fetched with the '.NS' suffix (e.g. 'RELIANCE.NS').
    Yahoo Finance does NOT provide NSE futures data directly, so we
    compute a THEORETICAL futures price using the cost-of-carry model:
        Futures = Spot × e^(r × T)
    where r = RBI repo rate + equity premium (~10% p.a. default).
    This is an approximation — use HybridProvider for more accuracy.
    """
    RATE_OF_CARRY = 0.10   # 10% p.a. (approx cost of carry in India)

    def __init__(self, cfg: dict):
        if not YFINANCE_OK:
            raise RuntimeError("yfinance not installed. Run: pip install yfinance")
        self.cfg   = cfg
        self._cache: Dict[str, float] = {}

    def _next_expiry(self) -> Tuple[str, int]:
        """Last Thursday of current (or next) month."""
        now = datetime.now()
        for month_offset in range(3):
            m = (now.month - 1 + month_offset) % 12 + 1
            y = now.year + (now.month - 1 + month_offset) // 12
            # find last Thursday in that month
            last_day = (datetime(y, m % 12 + 1, 1) - timedelta(days=1) if m < 12
                        else datetime(y + 1, 1, 1) - timedelta(days=1))
            d = last_day
            while d.weekday() != 3:   # 3 = Thursday
                d -= timedelta(days=1)
            days = (d - now).days
            if days >= self.cfg["MIN_DAYS_TO_EXPIRY"]:
                return d.strftime("%d-%b-%Y"), days
        return (now + timedelta(days=20)).strftime("%d-%b-%Y"), 20

    def get_spot(self, symbol: str) -> Optional[float]:
        if symbol in self._cache:
            return self._cache[symbol]
        try:
            ticker = yf.Ticker(f"{symbol}.NS")
            hist   = ticker.history(period="1d")
            if hist.empty:
                return None
            price = float(hist["Close"].iloc[-1])
            self._cache[symbol] = price
            return price
        except Exception:
            return None

    def theoretical_futures(self, spot: float, days: int) -> float:
        T = days / 365
        return round(spot * math.exp(self.RATE_OF_CARRY * T), 2)

    def get_quote(self, symbol: str) -> Optional[Quote]:
        try:
            spot = self.get_spot(symbol)
            if not spot or spot <= 0:
                return None
            expiry, days = self._next_expiry()
            fut  = self.theoretical_futures(spot, days)
            lot  = NSE_LOT_SIZES.get(symbol, DEFAULT_LOT)
            return Quote(symbol=symbol, spot=spot, futures=fut,
                         lot_size=lot, expiry=expiry, days=days,
                         source="YFinance+Theoretical")
        except Exception:
            return None

    def name(self) -> str: return "Yahoo Finance (yfinance)"


# ─────────────────────────────────────────────────────────────────────────────
# PROVIDER 3 — HYBRID  (yfinance spot  +  NSE futures)
# ─────────────────────────────────────────────────────────────────────────────
class HybridProvider:
    """
    Most accurate free combo:
    • Spot  → yfinance  (very reliable, rarely fails)
    • Futures price + expiry + lot size → NSE public API
    This gives real futures mispricing data, not theoretical prices.
    """
    def __init__(self, cfg: dict):
        self._yf  = YFinanceProvider(cfg)
        self._nse = NSEProvider(cfg)
        self.cfg  = cfg

    def get_quote(self, symbol: str) -> Optional[Quote]:
        try:
            spot = self._yf.get_spot(symbol)
            if not spot or spot <= 0:
                return None
            time.sleep(self.cfg["NSE_DELAY_SEC"])
            fut_price, expiry, days, lot = self._nse.get_futures(symbol)
            if not fut_price or days < 1:
                # Fallback to theoretical
                expiry, days = self._yf._next_expiry()
                fut_price = self._yf.theoretical_futures(spot, days)
                lot = NSE_LOT_SIZES.get(symbol, DEFAULT_LOT)
                src = "YFinance+Theoretical"
            else:
                src = "Hybrid(YF+NSE)"
            return Quote(symbol=symbol, spot=spot, futures=fut_price,
                         lot_size=lot, expiry=expiry, days=days, source=src)
        except Exception:
            return None

    def name(self) -> str: return "Hybrid (yfinance spot + NSE futures)"


# ─────────────────────────────────────────────────────────────────────────────
# PROVIDER 4 — MOCK  (built-in, always works offline — great for testing)
# ─────────────────────────────────────────────────────────────────────────────
class MockProvider:
    """
    Generates realistic simulated data.
    Futures = Spot × e^(r×T) where r ~ 7-16% + random noise.
    8% chance of a "fat spread" to simulate real arbitrage windows.
    """
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self._expiry, self._days = self._calc_expiry()

    def _calc_expiry(self) -> Tuple[str, int]:
        now = datetime.now()
        # Approximate last Thursday of current month
        m, y = now.month, now.year
        last_day = datetime(y, m % 12 + 1, 1) - timedelta(days=1) if m < 12 \
                   else datetime(y + 1, 1, 1) - timedelta(days=1)
        d = last_day
        while d.weekday() != 3:
            d -= timedelta(days=1)
        days = max(5, (d - now).days)
        if days < self.cfg["MIN_DAYS_TO_EXPIRY"]:
            d += timedelta(days=30)
            while d.weekday() != 3: d += timedelta(days=1)
            days = (d - now).days
        return d.strftime("%d-%b-%Y"), days

    def _gauss(self, sigma: float) -> float:
        u, v = random.random(), random.random()
        while u == 0: u = random.random()
        while v == 0: v = random.random()
        return sigma * math.sqrt(-2 * math.log(u)) * math.cos(2 * math.pi * v)

    def get_quote(self, symbol: str) -> Optional[Quote]:
        base  = MOCK_SPOT.get(symbol, random.uniform(200, 8000))
        spot  = round(max(10, base + self._gauss(base * 0.005)), 2)
        r     = random.uniform(0.07, 0.16)
        T     = self._days / 365
        fut   = round(spot * math.exp(r * T) + self._gauss(spot * 0.002), 2)
        # Occasionally simulate a juicy spread
        if random.random() < 0.08:
            fut = round(fut * (1 + random.uniform(0.005, 0.025)), 2)
        if fut <= spot:
            return None
        lot = NSE_LOT_SIZES.get(symbol, DEFAULT_LOT)
        return Quote(symbol=symbol, spot=spot, futures=fut,
                     lot_size=lot, expiry=self._expiry,
                     days=self._days, source="Mock")

    def name(self) -> str: return "Mock (built-in offline data)"


# ─────────────────────────────────────────────────────────────────────────────
# AUTO-DETECT: pick best available provider
# ─────────────────────────────────────────────────────────────────────────────
def auto_detect_provider(cfg: dict, requested: str = "auto"):
    """
    Provider priority when 'auto':
      1. Hybrid  (yfinance + NSE)  — most reliable combo
      2. YFinance alone            — if NSE is rate-limiting
      3. NSE alone                 — if yfinance fails
      4. Mock                      — always works

    Pass --provider nse/yfinance/hybrid/mock to override.
    """
    label = requested.lower()

    if label == "mock":
        return MockProvider(cfg)

    if label == "nse":
        if not REQUESTS_OK:
            print("[WARN] requests not installed → falling back to Mock")
            return MockProvider(cfg)
        return NSEProvider(cfg)

    if label == "yfinance":
        if not YFINANCE_OK:
            print("[WARN] yfinance not installed → falling back to Mock")
            return MockProvider(cfg)
        return YFinanceProvider(cfg)

    if label == "hybrid":
        if YFINANCE_OK and REQUESTS_OK:
            return HybridProvider(cfg)
        if YFINANCE_OK:
            return YFinanceProvider(cfg)
        if REQUESTS_OK:
            return NSEProvider(cfg)
        return MockProvider(cfg)

    # auto — try best available
    if YFINANCE_OK and REQUESTS_OK:
        print("[AUTO] Using Hybrid provider (yfinance spot + NSE futures)")
        return HybridProvider(cfg)
    if YFINANCE_OK:
        print("[AUTO] Using YFinance provider (spot + theoretical futures)")
        return YFinanceProvider(cfg)
    if REQUESTS_OK:
        print("[AUTO] Using NSE public API provider")
        return NSEProvider(cfg)
    print("[AUTO] No network libs found → using Mock provider")
    return MockProvider(cfg)


# ─────────────────────────────────────────────────────────────────────────────
# SCANNER ENGINE
# ─────────────────────────────────────────────────────────────────────────────
class Scanner:
    def __init__(self, provider, cfg: dict):
        self.provider   = provider
        self.cfg        = cfg
        self.cost       = CostCalc(cfg)
        self._results:  List[Opportunity] = []
        self._lock      = threading.Lock()

    def _classify(self, ann: float) -> str:
        if ann >= 18: return "STRONG BUY"
        if ann >= 12: return "BUY"
        if ann >= 8:  return "WATCH"
        return "SKIP"

    def scan_symbol(self, symbol: str) -> Optional[Opportunity]:
        q = self.provider.get_quote(symbol)
        if not q or q.spot <= 0 or q.futures <= 0:
            return None
        if q.days < self.cfg["MIN_DAYS_TO_EXPIRY"]: return None
        if q.days > self.cfg["MAX_DAYS_TO_EXPIRY"]:  return None

        raw = (q.futures - q.spot) / q.spot * 100
        if raw < self.cfg["MIN_SPREAD_PCT"]:  return None
        if raw > self.cfg["MAX_SPREAD_PCT"]:  return None

        cost    = self.cost.total_cost_pct(q.spot, q.futures, q.lot_size)
        net     = self.cost.net_spread(raw, cost)
        ann     = self.cost.annualized(net, q.days)
        if ann < self.cfg["MIN_ANNUALIZED_PCT"]: return None

        margin  = self.cost.margin_required(q.futures, q.lot_size)
        profit  = self.cost.max_profit_per_lot(q.spot, net, q.lot_size)
        signal  = self._classify(ann)

        return Opportunity(
            symbol=q.symbol, spot=q.spot, futures=q.futures,
            raw_spread_pct=round(raw, 3), cost_pct=round(cost, 3),
            net_spread_pct=round(net, 3), ann_pct=round(ann, 1),
            days=q.days, lot_size=q.lot_size,
            margin_rs=margin, profit_per_lot=profit,
            expiry=q.expiry, timestamp=datetime.now().strftime("%H:%M:%S"),
            signal=signal, source=q.source,
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
        self.cfg       = cfg
        self.cost      = CostCalc(cfg)
        self.portfolio = Portfolio(capital=cfg["PAPER_CAPITAL_RS"])
        self._pid      = 0
        self._switch_ts: Dict[str, float] = {}
        self._day_reset = datetime.now().date()

    def _new_pid(self) -> str:
        self._pid += 1
        return f"POS-{self._pid:04d}"

    def _check_risk(self) -> Tuple[bool, str]:
        today = datetime.now().date()
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
        ok, reason = self._check_risk()
        if not ok:
            return None
        if opp.symbol in self.portfolio.positions:
            return None   # already have it

        cap   = min(self.cfg["MAX_POSITION_SIZE_RS"], self.portfolio.cash)
        lots  = max(1, int(cap / (opp.spot * opp.lot_size)))
        total = (opp.spot * opp.lot_size * lots +
                 self.cost.margin_required(opp.futures, opp.lot_size, lots))
        if total > self.portfolio.cash:
            lots = max(1, lots - 1)
            total = (opp.spot * opp.lot_size * lots +
                     self.cost.margin_required(opp.futures, opp.lot_size, lots))
        if total > self.portfolio.cash:
            return None

        pos = Position(
            pid=self._new_pid(), symbol=opp.symbol,
            entry_spot=opp.spot, entry_fut=opp.futures,
            entry_spread_pct=opp.raw_spread_pct,
            net_spread_pct=opp.net_spread_pct,
            lots=lots, lot_size=opp.lot_size,
            capital_rs=total, entry_time=datetime.now().strftime("%Y-%m-%d %H:%M"),
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
            # Check expiry
            try:
                dt = datetime.strptime(pos.expiry, "%d-%b-%Y")
                days_left = (dt - datetime.now()).days
            except Exception:
                days_left = 5
            if days_left <= 0:
                self._close(pos, "EXPIRY")
                continue
            cur = omap.get(sym)
            if cur:
                # Update unrealized P&L
                spread_gain = pos.entry_spread_pct - cur.raw_spread_pct
                pos.unrealized_pnl = round(
                    spread_gain / 100 * pos.entry_spot * pos.lot_size * pos.lots, 2)
                # Stop-loss
                if cur.raw_spread_pct < self.cfg["STOP_LOSS_SPREAD_PCT"]:
                    self._close(pos, "STOP_LOSS",
                                exit_spot=cur.spot, exit_fut=cur.futures)
                    continue
                # Switching
                self._try_switch(pos, opps)

    def _try_switch(self, pos: Position, opps: List[Opportunity]):
        now = time.time()
        if pos.symbol in self._switch_ts:
            if now - self._switch_ts[pos.symbol] < self.cfg["SWITCH_COOLDOWN_SEC"]:
                return
        better = next((o for o in opps
                       if o.symbol != pos.symbol
                       and o.symbol not in self.portfolio.positions
                       and o.net_spread_pct > pos.net_spread_pct
                                               + self.cfg["SWITCH_THRESHOLD_PCT"]), None)
        if better:
            self._close(pos, f"SWITCH→{better.symbol}")
            self._switch_ts[pos.symbol] = now
            self.try_enter(better)

    def _close(self, pos: Position, reason: str = "MANUAL",
               exit_spot: float = None, exit_fut: float = None):
        if reason == "EXPIRY":
            # Futures converge to spot at expiry
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

        pos.exit_spot   = round(es, 2)
        pos.exit_fut    = round(ef, 2)
        pos.exit_time   = datetime.now().strftime("%Y-%m-%d %H:%M")
        pos.exit_reason = reason
        pos.realized_pnl = net_pnl
        pos.unrealized_pnl = 0.0
        pos.status      = "CLOSED"

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
                w.writerow([o.timestamp,o.rank,o.symbol,o.spot,o.futures,
                             o.raw_spread_pct,o.cost_pct,o.net_spread_pct,
                             o.ann_pct,o.days,o.lot_size,o.margin_rs,
                             o.profit_per_lot,o.expiry,o.signal,o.source])

    def trade(self, pos: Position):
        with open(self.trades_file, "a", newline="") as f:
            csv.writer(f).writerow([
                pos.pid,pos.symbol,pos.entry_spot,pos.entry_fut,
                pos.entry_spread_pct,pos.net_spread_pct,pos.lots,pos.lot_size,
                pos.capital_rs,pos.entry_time,pos.expiry,
                pos.exit_spot or "",pos.exit_fut or "",pos.exit_time or "",
                pos.exit_reason,pos.realized_pnl])

    def pnl(self, p: Portfolio):
        wr = round(p.wins / p.trades * 100, 1) if p.trades > 0 else 0
        with open(self.pnl_file, "a", newline="") as f:
            csv.writer(f).writerow([
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                round(p.total_pnl, 2), round(p.daily_pnl, 2),
                round(p.cash, 2), round(p.deployed, 2),
                len(p.positions), p.trades, wr])


# ─────────────────────────────────────────────────────────────────────────────
# TERMINAL DASHBOARD  (rich)
# ─────────────────────────────────────────────────────────────────────────────
class Dashboard:
    def __init__(self, cfg: dict, provider_name: str):
        self.cfg    = cfg
        self.pname  = provider_name
        self.con    = Console() if RICH_OK else None

    @staticmethod
    def _sig_style(sig: str) -> str:
        return {"STRONG BUY":"bold green","BUY":"green",
                "WATCH":"yellow","SKIP":"dim"}.get(sig, "white")

    @staticmethod
    def _pnl_style(v: float) -> str:
        return "bold green" if v > 0 else "bold red" if v < 0 else "white"

    def render(self, opps: List[Opportunity],
               port: Portfolio, scan_n: int):
        if not RICH_OK:
            self._plain(opps, port, scan_n)
            return None

        now  = datetime.now().strftime("%d-%b-%Y  %H:%M:%S")
        wr   = round(port.wins / port.trades * 100, 1) if port.trades else 0
        pc   = "green"   if port.total_pnl >= 0 else "red"
        dpc  = "green"   if port.daily_pnl >= 0 else "red"

        # Header
        hdr = Panel(
            f"[bold cyan]⚡ CASH & CARRY ARB SCANNER[/bold cyan]  "
            f"[dim]Indian F&O | {self.pname} | Paper Mode[/dim]  "
            f"[yellow]Scan #{scan_n}  {now}[/yellow]",
        )

        # Portfolio bar
        pt = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
        for _ in range(6): pt.add_column()
        pt.add_row(
            "Capital",  f"₹{port.capital:,.0f}",
            "Cash",     f"₹{port.cash:,.0f}",
            "Deployed", f"₹{port.deployed:,.0f}",
        )
        pt.add_row(
            "Total P&L", f"[{pc}]₹{port.total_pnl:+,.2f}[/{pc}]",
            "Daily P&L", f"[{dpc}]₹{port.daily_pnl:+,.2f}[/{dpc}]",
            "Positions", f"{len(port.positions)} / {self.cfg['MAX_OPEN_POSITIONS']}",
        )
        pt.add_row(
            "Trades", str(port.trades),
            "Winners", str(port.wins),
            "Win rate", f"{wr:.1f}%",
        )
        pp = Panel(pt, title="[bold]Portfolio[/bold]", border_style="blue")

        # Opportunities table
        ot = Table(
            box=box.SIMPLE_HEAVY, show_header=True,
            header_style="bold cyan",
            title=f"[bold]Top Arb Opportunities[/bold]  "
                  f"[dim](min {self.cfg['MIN_ANNUALIZED_PCT']}% ann, "
                  f"source: {self.pname})[/dim]",
        )
        cols = ["#","Symbol","Spot ₹","Futures ₹","Raw%","Cost%","Net%",
                "Ann%","Days","Lot","Margin ₹","Profit/lot ₹","Expiry","Signal"]
        widths = [3,14,10,11,7,7,7,7,5,5,10,13,11,11]
        rights = {2,3,4,5,6,7,8,9,10,11}
        for i,(c,w) in enumerate(zip(cols, widths)):
            ot.add_column(c, width=w, justify="right" if i in rights else "left")

        for o in opps[:self.cfg["TOP_N_DISPLAY"]]:
            ac = "green" if o.ann_pct >= 12 else "yellow"
            ot.add_row(
                str(o.rank), o.symbol,
                f"{o.spot:,.2f}", f"{o.futures:,.2f}",
                f"{o.raw_spread_pct:.3f}",
                f"[dim]{o.cost_pct:.3f}[/dim]",
                f"{o.net_spread_pct:.3f}",
                f"[{ac}]{o.ann_pct:.1f}[/{ac}]",
                str(o.days), str(o.lot_size),
                f"{o.margin_rs:,.0f}",
                f"{o.profit_per_lot:+,.0f}",
                o.expiry,
                f"[{self._sig_style(o.signal)}]{o.signal}[/{self._sig_style(o.signal)}]",
            )

        parts = [hdr, pp, ot]

        # Open positions
        if port.positions:
            pos_t = Table(box=box.SIMPLE_HEAVY, show_header=True,
                          header_style="bold magenta",
                          title="[bold]Open Positions[/bold]")
            for c in ["ID","Symbol","Spot","Fut","Spread%","Lots","Capital ₹","Unrealized ₹","Expiry"]:
                pos_t.add_column(c)
            for pos in port.positions.values():
                us = self._pnl_style(pos.unrealized_pnl)
                pos_t.add_row(
                    pos.pid, pos.symbol, f"{pos.entry_spot:.2f}",
                    f"{pos.entry_fut:.2f}", f"{pos.entry_spread_pct:.3f}",
                    str(pos.lots), f"{pos.capital_rs:,.0f}",
                    f"[{us}]{pos.unrealized_pnl:+,.2f}[/{us}]",
                    pos.expiry,
                )
            parts.append(pos_t)

        return Group(*parts)

    def _plain(self, opps, port, scan_n):
        sep = "═" * 110
        print(f"\n{sep}")
        print(f"  CASH & CARRY ARB  |  {self.pname}  |  Scan #{scan_n}  |  "
              f"{datetime.now().strftime('%H:%M:%S')}")
        print(f"  Capital ₹{port.capital:,.0f}  Cash ₹{port.cash:,.0f}  "
              f"P&L ₹{port.total_pnl:+,.2f}  Open {len(port.positions)}")
        print(sep)
        hdr = f"  {'#':3} {'Symbol':14} {'Spot':>9} {'Futures':>10} {'Raw%':>6} " \
              f"{'Cost%':>6} {'Net%':>6} {'Ann%':>6} {'Days':>4} {'Lot':>5}  Signal"
        print(hdr)
        print("─" * 110)
        for o in opps[:self.cfg["TOP_N_DISPLAY"]]:
            print(f"  {o.rank:3} {o.symbol:14} {o.spot:>9.2f} {o.futures:>10.2f} "
                  f"{o.raw_spread_pct:>6.3f} {o.cost_pct:>6.3f} "
                  f"{o.net_spread_pct:>6.3f} {o.ann_pct:>6.1f} "
                  f"{o.days:>4} {o.lot_size:>5}  {o.signal}")
        print(sep)


# ─────────────────────────────────────────────────────────────────────────────
# MARKET HOURS CHECK
# ─────────────────────────────────────────────────────────────────────────────
def is_market_hours() -> bool:
    """NSE market hours: Mon–Fri, 09:15–15:30 IST."""
    now = datetime.now()
    if now.weekday() >= 5:          # Saturday, Sunday
        return False
    market_open  = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return market_open <= now <= market_close


# ─────────────────────────────────────────────────────────────────────────────
# MAIN BOT ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────
class ArbBot:
    def __init__(self, cfg: dict, provider_name: str = "auto"):
        self.cfg        = cfg
        self.provider   = auto_detect_provider(cfg, provider_name)
        self.scanner    = Scanner(self.provider, cfg)
        self.trader     = PaperTrader(cfg)
        self.logger     = Logger(cfg)
        self.dash       = Dashboard(cfg, self.provider.name())
        self.scan_n     = 0
        self._running   = False

    def _cycle(self) -> List[Opportunity]:
        self.scan_n += 1
        self.trader.portfolio.scans = self.scan_n

        opps = self.scanner.scan_all()
        self.trader.update(opps)

        # Paper trade: enter top STRONG BUY / BUY signals
        for opp in opps[:5]:
            if opp.signal in ("STRONG BUY", "BUY"):
                pos = self.trader.try_enter(opp)
                if pos:
                    self.logger.trade(pos)

        self.logger.scan(opps[:self.cfg["TOP_N_DISPLAY"]])
        self.logger.pnl(self.trader.portfolio)
        return opps

    def run(self):
        print(f"\n{'═'*70}")
        print(f"  CASH & CARRY ARB SCANNER  —  PAPER TRADING MODE")
        print(f"{'═'*70}")
        print(f"  Data source   : {self.provider.name()}")
        print(f"  Universe      : {len(FNO_UNIVERSE)} F&O stocks")
        print(f"  Scan interval : {self.cfg['SCAN_INTERVAL_SECONDS']}s")
        print(f"  Min ann. yield: {self.cfg['MIN_ANNUALIZED_PCT']}%")
        print(f"  Paper capital : ₹{self.cfg['PAPER_CAPITAL_RS']:,.0f}")
        print(f"  Logs          : {self.cfg['LOG_DIR']}/")
        print(f"{'─'*70}")
        print("  Press Ctrl+C to stop\n")

        if not is_market_hours():
            print("  [WARN] Market is currently closed. Prices may be stale.")
            print("         NSE hours: Mon–Fri 09:15–15:30 IST\n")

        self._running = True

        if RICH_OK:
            con = Console()
            with Live(console=con, refresh_per_second=0.5, screen=False) as live:
                while self._running:
                    try:
                        opps = self._cycle()
                        rendered = self.dash.render(
                            opps, self.trader.portfolio, self.scan_n)
                        if rendered:
                            live.update(rendered)
                        # Interruptible sleep
                        for _ in range(self.cfg["SCAN_INTERVAL_SECONDS"] * 2):
                            if not self._running: break
                            time.sleep(0.5)
                    except KeyboardInterrupt:
                        self._running = False
        else:
            while self._running:
                try:
                    opps = self._cycle()
                    self.dash.render(opps, self.trader.portfolio, self.scan_n)
                    time.sleep(self.cfg["SCAN_INTERVAL_SECONDS"])
                except KeyboardInterrupt:
                    self._running = False

        self._summary()

    def run_once(self):
        print(f"\nRunning single scan with {self.provider.name()} …")
        opps = self._cycle()
        if RICH_OK:
            Console().print(
                self.dash.render(opps, self.trader.portfolio, self.scan_n))
        else:
            self.dash.render(opps, self.trader.portfolio, self.scan_n)
        return opps

    def _summary(self):
        p = self.trader.portfolio
        wr = round(p.wins / p.trades * 100, 1) if p.trades else 0
        print(f"\n{'═'*55}")
        print("  SESSION SUMMARY")
        print(f"{'═'*55}")
        print(f"  Provider  : {self.provider.name()}")
        print(f"  Scans     : {self.scan_n}")
        print(f"  Trades    : {p.trades}")
        print(f"  Win rate  : {wr}%")
        print(f"  Total P&L : ₹{p.total_pnl:+,.2f}")
        print(f"  Final cash: ₹{p.cash:,.0f}")
        print(f"  Logs      : {self.cfg['LOG_DIR']}/")
        print(f"{'═'*55}")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser(
        description="Cash & Carry Arb Scanner — Indian F&O  (100% free, no broker key)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
DATA PROVIDERS (all free, no API key):
  auto      — picks best available (default)
  hybrid    — yfinance spot + NSE futures  [RECOMMENDED]
  nse       — NSE India public REST API (spot + real futures)
  yfinance  — Yahoo Finance (spot + theoretical futures)
  mock      — built-in fake data, works fully offline

INSTALL:
  pip install yfinance nsepython requests pandas rich

EXAMPLES:
  python arb_scanner.py                        auto-detect, run continuously
  python arb_scanner.py --provider hybrid      yfinance + NSE
  python arb_scanner.py --provider nse --once  one scan from NSE, then exit
  python arb_scanner.py --provider mock        offline test, no internet
  python arb_scanner.py --min-ann 12           only show >= 12% annualized
  python arb_scanner.py --interval 30          scan every 30 seconds
  python arb_scanner.py --capital 1000000      Rs 10 lakh paper capital
  python arb_scanner.py --top 25               show top 25 rows
""")
    p.add_argument("--provider", default="auto",
                   choices=["auto","hybrid","nse","yfinance","mock"],
                   help="Data source (default: auto)")
    p.add_argument("--once",      action="store_true", help="Single scan then exit")
    p.add_argument("--min-ann",   type=float, help="Minimum annualized return %%")
    p.add_argument("--min-spread",type=float, help="Minimum raw spread %%")
    p.add_argument("--interval",  type=int,   help="Scan interval in seconds")
    p.add_argument("--capital",   type=float, help="Paper trading capital in ₹")
    p.add_argument("--top",       type=int,   help="Number of rows to display")
    args, _ = p.parse_known_args()   # parse_known_args ignores Jupyter kernel args

    cfg = CONFIG.copy()
    if args.min_ann:    cfg["MIN_ANNUALIZED_PCT"]   = args.min_ann
    if args.min_spread: cfg["MIN_SPREAD_PCT"]        = args.min_spread
    if args.interval:   cfg["SCAN_INTERVAL_SECONDS"] = args.interval
    if args.capital:    cfg["PAPER_CAPITAL_RS"]      = args.capital
    if args.top:        cfg["TOP_N_DISPLAY"]         = args.top

    bot = ArbBot(cfg, provider_name=args.provider)
    if args.once:
        bot.run_once()
    else:
        bot.run()


if __name__ == "__main__":
    main()