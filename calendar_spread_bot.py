#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================
 CALENDAR SPREAD ARBITRAGE BOT  --  NSE INDIA  --  PAPER TRADE
================================================================
 Strategy : Calendar Spread Arbitrage
   * Scan current-month vs next-month futures for every F&O name
   * Rank by theoretical max-profit % on margin (best first)
   * Volume filter : 1-min vol of far-month >= 150 x position size
   * ENTER only when max-profit potential >= 10% on margin used
   * EXIT  when net P&L hits +10% of margin used
   * Show live Top-10 table + active-position PnL every minute

 Data Source : NSE India public API  (free, no login)

 HTTP layer  : stdlib only  (urllib + http.cookiejar)
               NO requests library needed.

 pip install : pip install rich
================================================================
"""

# ----------------------------------------------------------------
#  STANDARD LIBRARY  -- everything needed is built-in
# ----------------------------------------------------------------
import sys
import time
import math
import json
import gzip
import zlib
import io
import datetime
import ssl
import urllib.request
import urllib.error
import http.cookiejar
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

# ----------------------------------------------------------------
#  THIRD-PARTY  -- only rich (for terminal UI)
# ----------------------------------------------------------------
try:
    from rich.console import Console
    from rich.table   import Table
    from rich.panel   import Panel
    from rich         import box
except ImportError:
    print("\n  Missing library: rich")
    print("  Run:  pip install rich\n")
    sys.exit(1)

# Quick console for startup messages before bot fully initialises
_con = Console()

# ================================================================
#  CONFIGURATION
# ================================================================
INITIAL_CAPITAL      = 40_000   # INR starting paper capital
PROFIT_TARGET_PCT    = 10.0     # EXIT  when net PnL >= 10% of margin used
MIN_MAX_PROFIT_PCT   = 10.0     # ENTER only when max theoretical profit >= 10% on margin
VOLUME_FILTER_MULT   = 150      # far-month 1-min vol must be >= 150 x position qty
SCAN_INTERVAL_SEC    = 60       # seconds between full scans
TOP_N                = 10       # display top-N spread opportunities
MAX_OPEN_POSITIONS   = 3        # max simultaneous spread positions

# Calendar spread margin ~4% of near-month contract value
CALENDAR_MARGIN_PCT  = 0.04

# --  Transaction cost components (realistic NSE estimates)  -----
BROKERAGE_PER_ORDER  = 20.0         # flat fee per order
STT_FUTURES_PCT      = 0.0125 / 100 # 0.0125% on SELL side turnover
EXCHANGE_CHARGES_PCT = 0.0019 / 100 # NSE transaction charge
SEBI_CHARGES_PCT     = 0.0001 / 100
GST_RATE             = 0.18
STAMP_DUTY_PCT       = 0.002  / 100 # on BUY side turnover

# ================================================================
#  F&O UNIVERSE
# ================================================================
FNO_INDICES = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]

FNO_STOCKS = [
    "RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "WIPRO",
    "HINDUNILVR", "SBIN", "BHARTIARTL", "ITC", "KOTAKBANK", "AXISBANK",
    "BAJFINANCE", "HCLTECH", "ASIANPAINT", "MARUTI", "TITAN", "NESTLEIND",
    "NTPC", "COALINDIA", "ONGC", "DRREDDY", "SUNPHARMA", "TECHM",
    "ADANIENT", "ADANIPORTS", "TATAMOTORS", "TATASTEEL", "JSWSTEEL",
    "HINDALCO", "VEDL", "CIPLA", "DIVISLAB", "APOLLOHOSP", "BAJAJFINSV",
    "SBILIFE", "HDFCLIFE", "GRASIM", "ULTRACEMCO", "HEROMOTOCO",
    "BAJAJ-AUTO", "EICHERMOT", "M&M", "LT", "CANBK", "PNB", "BANKBARODA",
    "BEL", "HAL", "IRCTC", "ZOMATO", "DMART", "TATACONSUM", "POWERGRID",
    "INDUSINDBK", "BANDHANBNK", "MUTHOOTFIN", "CHOLAFIN", "PFC", "RECLTD",
    "ESCORTS", "ASHOKLEY", "TVSMOTOR", "PIDILITIND", "HAVELLS", "ICICIPRULI",
    "FEDERALBNK", "BERGEPAINT", "VOLTAS", "SIEMENS", "ABB", "BHEL",
    "IRFC", "CONCOR", "NATIONALUM", "SAIL", "NMDC", "GLENMARK",
    "ALKEM", "TORNTPHARM", "LUPIN", "BIOCON", "ABBOTINDIA",
]

# Approximate NSE lot sizes -- updated from live API when available
DEFAULT_LOT_SIZES: Dict[str, int] = {
    "NIFTY": 50,  "BANKNIFTY": 15, "FINNIFTY": 40, "MIDCPNIFTY": 75,
    "RELIANCE": 250,  "TCS": 175,  "INFY": 300,   "HDFCBANK": 550,
    "ICICIBANK": 700, "WIPRO": 1500,"HINDUNILVR": 300, "SBIN": 1500,
    "BHARTIARTL": 950,"ITC": 1600, "KOTAKBANK": 400,  "AXISBANK": 625,
    "BAJFINANCE": 125,"HCLTECH": 700,"ASIANPAINT": 200,"MARUTI": 100,
    "TITAN": 375, "NESTLEIND": 50, "POWERGRID": 4700, "NTPC": 3000,
    "COALINDIA": 4200,"ONGC": 3850, "DRREDDY": 125,   "SUNPHARMA": 700,
    "TECHM": 600, "ADANIENT": 250, "ADANIPORTS": 1250, "TATAMOTORS": 2000,
    "TATASTEEL": 5500,"JSWSTEEL": 600,"HINDALCO": 2150,"VEDL": 2000,
    "CIPLA": 650, "DIVISLAB": 200, "APOLLOHOSP": 125,  "BAJAJFINSV": 125,
    "SBILIFE": 750,"HDFCLIFE": 1100,"GRASIM": 375,     "ULTRACEMCO": 100,
    "HEROMOTOCO": 300,"BAJAJ-AUTO": 75,"EICHERMOT": 75,"M&M": 700,
    "LT": 300,  "CANBK": 8100, "PNB": 8000,  "BANKBARODA": 5850,
    "BEL": 3700,"HAL": 150,    "IRCTC": 1375,"IRFC": 12500,
    "ZOMATO": 4500,"DMART": 165,"TATACONSUM": 1000,"INDUSINDBK": 500,
    "BANDHANBNK": 5000,"MUTHOOTFIN": 1000,"CHOLAFIN": 1250,"PFC": 4600,
    "RECLTD": 2500,"ESCORTS": 550,"ASHOKLEY": 5000,"TVSMOTOR": 350,
    "PIDILITIND": 275,"HAVELLS": 500,"ICICIPRULI": 1500,"FEDERALBNK": 5000,
    "BERGEPAINT": 1100,"VOLTAS": 500,"SIEMENS": 350,"ABB": 250,
    "BHEL": 10500,"CONCOR": 1000,"NATIONALUM": 8000,"SAIL": 13000,
    "NMDC": 8000,"GLENMARK": 625,"ALKEM": 200,"TORNTPHARM": 500,
    "LUPIN": 700,"BIOCON": 2000,"ABBOTINDIA": 150,
}

# ================================================================
#  DATA MODELS
# ================================================================
@dataclass
class FutureQuote:
    symbol:        str
    expiry:        str
    last_price:    float
    daily_volume:  int
    lot_size:      int
    open_interest: int
    high:          float
    low:           float
    prev_close:    float


@dataclass
class SpreadOpp:
    symbol:          str
    is_index:        bool
    near:            FutureQuote
    far:             FutureQuote
    spread_abs:      float   # far.price - near.price
    spread_pct:      float   # (far - near) / near * 100
    max_profit_pct:  float   # theoretical max return on margin if spread -> 0
    margin_required: float   # estimated margin for 1-lot calendar spread
    volume_ok:       bool
    can_trade:       bool    # margin_ok AND volume_ok AND max_profit_pct >= threshold


@dataclass
class Trade:
    trade_id:       str
    symbol:         str
    is_index:       bool
    direction:      str      # "BUY_NEAR_SELL_FAR" or "SELL_NEAR_BUY_FAR"
    near_expiry:    str
    far_expiry:     str
    near_entry:     float
    far_entry:      float
    lot_size:       int
    lots:           int
    entry_spread:   float
    entry_time:     datetime.datetime
    margin_used:    float

    # updated each scan
    near_now:       float = 0.0
    far_now:        float = 0.0
    current_spread: float = 0.0
    gross_pnl:      float = 0.0
    net_pnl:        float = 0.0
    net_pnl_pct:    float = 0.0

    # filled on exit
    is_closed:      bool  = False
    exit_time:      Optional[datetime.datetime] = None
    exit_near:      float = 0.0
    exit_far:       float = 0.0
    final_net_pnl:  float = 0.0
    exit_reason:    str   = ""


# ================================================================
#  TRANSACTION COST CALCULATOR
# ================================================================
def calc_costs(price: float, lot_size: int, lots: int) -> float:
    """
    Round-trip transaction costs for a calendar spread (4 order legs):
        Open  : buy near  + sell far
        Close : sell near + buy far
    """
    qty   = lot_size * lots
    value = price * qty

    brokerage = BROKERAGE_PER_ORDER * 4          # 4 orders
    stt       = value * STT_FUTURES_PCT * 2       # sell side x2
    exchange  = value * EXCHANGE_CHARGES_PCT * 4
    sebi      = value * SEBI_CHARGES_PCT * 4
    stamp     = value * STAMP_DUTY_PCT * 2        # buy side x2
    gst       = (brokerage + exchange) * GST_RATE

    return brokerage + stt + exchange + sebi + stamp + gst


# ================================================================
#  NSE SESSION  --  ONLY Python stdlib  (urllib + http.cookiejar)
#
#  KEY FIXES vs naive urllib usage:
#    1) Accept-Encoding asks for "gzip, deflate" only (NOT brotli,
#       because Python stdlib cannot decompress brotli).
#    2) _read_response() manually decompresses gzip/deflate,
#       because urllib does NOT auto-decompress (unlike requests).
#    3) Cookie seeding hits MULTIPLE NSE pages with realistic delays
#       to ensure the nseappid / bm_* cookies are properly set.
#    4) Verbose logging so user can see cookie/session status.
# ================================================================
class NSESession:
    """
    Persistent HTTP session using built-in urllib.
    Handles NSE cookie-based auth without any third-party library.
    """

    BASE = "https://www.nseindia.com"

    # -- IMPORTANT: only gzip+deflate, NOT brotli (br) --
    # urllib cannot decompress brotli; if we ask for it, NSE sends
    # back brotli-encoded bytes and json.loads fails with garbage.
    HEADERS_PAGE = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,image/avif,image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Connection":      "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest":  "document",
        "Sec-Fetch-Mode":  "navigate",
        "Sec-Fetch-Site":  "none",
        "Sec-Fetch-User":  "?1",
        "Cache-Control":   "max-age=0",
    }

    HEADERS_API = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        ),
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Referer":         "https://www.nseindia.com/get-quotes/derivatives?symbol=NIFTY",
        "Connection":      "keep-alive",
        "Sec-Fetch-Dest":  "empty",
        "Sec-Fetch-Mode":  "cors",
        "Sec-Fetch-Site":  "same-origin",
        "X-Requested-With": "XMLHttpRequest",
    }

    def __init__(self):
        # Cookie jar persists across all requests (like a browser session)
        self._jar = http.cookiejar.CookieJar()

        # SSL context
        self._ssl_ctx = ssl.create_default_context()
        self._ssl_ctx.check_hostname = False
        self._ssl_ctx.verify_mode    = ssl.CERT_NONE

        # Build opener: cookies auto-sent on every request
        self._opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self._jar),
            urllib.request.HTTPSHandler(context=self._ssl_ctx),
        )

        self._last_refresh     = 0.0
        self._refresh_interval = 180.0   # seconds -- refresh sooner to be safe
        self._cookies_ok       = False

        _con.print("[dim]NSE Session: initialising cookies...[/dim]")
        self._init_cookies()

    # ---- build request objects -----------------------------------
    def _page_request(self, url: str) -> urllib.request.Request:
        """Request with browser-page headers (for HTML pages)."""
        req = urllib.request.Request(url)
        for k, v in self.HEADERS_PAGE.items():
            req.add_header(k, v)
        return req

    def _api_request(self, url: str) -> urllib.request.Request:
        """Request with XHR/API headers (for JSON endpoints)."""
        req = urllib.request.Request(url)
        for k, v in self.HEADERS_API.items():
            req.add_header(k, v)
        return req

    # ---- decompress response body --------------------------------
    @staticmethod
    def _read_response(resp) -> bytes:
        """
        Read and decompress the response body.
        urllib does NOT auto-decompress gzip/deflate -- we must do it.
        """
        raw = resp.read()
        encoding = resp.headers.get("Content-Encoding", "").lower()

        if encoding == "gzip":
            try:
                return gzip.decompress(raw)
            except Exception:
                # Sometimes the header says gzip but data is raw
                return raw

        elif encoding == "deflate":
            try:
                return zlib.decompress(raw, -zlib.MAX_WBITS)
            except Exception:
                try:
                    return zlib.decompress(raw)
                except Exception:
                    return raw

        return raw

    # ---- cookie seeding ------------------------------------------
    def _init_cookies(self) -> bool:
        """
        Seed the cookie jar by visiting NSE pages like a real browser.
        NSE sets several cookies (nsit, nseappid, bm_sv, bm_sz, etc.)
        that are required before API endpoints return data.

        Flow:
          1) GET homepage  (sets initial cookies)
          2) GET /market-data/live-equity-market  (sometimes triggers more cookies)
          3) GET /get-quotes/derivatives?symbol=NIFTY  (final page seed)
          4) GET one API endpoint as a test

        Each step has a realistic delay to avoid being flagged.
        """
        seed_urls = [
            (self.BASE, "homepage"),
            (self.BASE + "/market-data/live-equity-market", "market-data"),
            (self.BASE + "/get-quotes/derivatives?symbol=NIFTY", "derivatives-page"),
        ]

        for url, label in seed_urls:
            try:
                req = self._page_request(url)
                with self._opener.open(req, timeout=15) as resp:
                    self._read_response(resp)  # consume body (needed for cookies)
                _con.print(
                    "  [green]OK[/green]  seeded from: [dim]{}[/dim]".format(label)
                )
                time.sleep(1.0 + 0.5 * seed_urls.index((url, label)))
            except Exception as exc:
                _con.print(
                    "  [red]FAIL[/red] seeding {}: {}".format(label, exc)
                )

        # Count cookies we got
        cookie_count = len(list(self._jar))
        _con.print(
            "  Cookies obtained: [bold]{}[/bold]".format(cookie_count)
        )

        if cookie_count > 0:
            # Quick test: try fetching one API endpoint
            test_url = self.BASE + "/api/quote-derivative?symbol=NIFTY"
            test_data = self._fetch_json_raw(test_url)
            if test_data is not None:
                _con.print(
                    "  [bold green]NSE session ready -- API test passed![/bold green]"
                )
                self._cookies_ok   = True
                self._last_refresh = time.time()
                return True
            else:
                _con.print(
                    "  [yellow]API test failed -- "
                    "will retry on next scan[/yellow]"
                )
        else:
            _con.print(
                "  [red]No cookies received -- "
                "NSE may be blocking or down[/red]"
            )

        self._last_refresh = time.time()
        return False

    def _maybe_refresh(self):
        """Refresh cookies if they have expired."""
        if time.time() - self._last_refresh > self._refresh_interval:
            _con.print(
                "[dim]NSE Session: refreshing cookies "
                "({}s elapsed)...[/dim]".format(
                    int(time.time() - self._last_refresh))
            )
            self._init_cookies()

    # ---- raw JSON fetch (internal) -------------------------------
    def _fetch_json_raw(self, url: str) -> Optional[dict]:
        """Single attempt to GET a JSON endpoint. No retries."""
        try:
            req = self._api_request(url)
            with self._opener.open(req, timeout=15) as resp:
                if resp.status != 200:
                    return None
                body = self._read_response(resp)
                text = body.decode("utf-8", errors="replace")
                return json.loads(text)
        except Exception:
            return None

    # ---- public: get JSON with retries --------------------------
    def get_json(self, url: str, retries: int = 3) -> Optional[dict]:
        """
        Fetch a JSON API endpoint with retry + auto cookie refresh.
        Uses only urllib -- no requests library.
        """
        self._maybe_refresh()

        for attempt in range(retries):
            try:
                req = self._api_request(url)
                with self._opener.open(req, timeout=15) as resp:
                    body = self._read_response(resp)
                    text = body.decode("utf-8", errors="replace")
                    return json.loads(text)

            except urllib.error.HTTPError as exc:
                if exc.code in (401, 403):
                    # Session expired or blocked -- re-seed cookies
                    if attempt < retries - 1:
                        self._init_cookies()
                        time.sleep(2)
                else:
                    time.sleep(1)

            except urllib.error.URLError:
                time.sleep(2)

            except json.JSONDecodeError:
                # Got response but it wasn't valid JSON (maybe HTML error page)
                if attempt < retries - 1:
                    self._init_cookies()
                    time.sleep(2)

            except Exception:
                time.sleep(1)

        return None


# ================================================================
#  NSE DATA FETCHER
# ================================================================
class NSEFetcher:
    """
    Fetches futures chain data from the NSE public derivative API.
    Endpoint: /api/quote-derivative?symbol=SYMBOL
    """

    DERIV_URL = "https://www.nseindia.com/api/quote-derivative?symbol={}"

    def __init__(self, session: NSESession):
        self.session  = session
        self.lot_cache: Dict[str, int] = dict(DEFAULT_LOT_SIZES)

    # ---- helpers ------------------------------------------------
    @staticmethod
    def _parse_expiry(date_str: str) -> datetime.datetime:
        for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d", "%d-%m-%Y"):
            try:
                return datetime.datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        return datetime.datetime.max

    @staticmethod
    def _f(val, default: float = 0.0) -> float:
        """Safe float conversion."""
        try:
            v = float(val)
            return v if math.isfinite(v) else default
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _i(val, default: int = 0) -> int:
        """Safe int conversion."""
        try:
            return int(float(val))
        except (TypeError, ValueError):
            return default

    # ---- fetch one symbol ---------------------------------------
    def fetch_chain(self, symbol: str) -> List[FutureQuote]:
        """Return all futures quotes for a symbol, sorted near-first."""
        data = self.session.get_json(self.DERIV_URL.format(symbol))
        if not data:
            return []

        quotes: List[FutureQuote] = []
        for item in data.get("stocks", []):
            meta  = item.get("metadata", {})
            instr = meta.get("instrumentType", "")

            # Keep futures rows only (skip options)
            if "Futures" not in instr:
                continue

            expiry = meta.get("expiryDate", "")
            if not expiry:
                continue

            price = self._f(meta.get("lastPrice"))
            if price <= 0:
                continue

            lot = self._i(meta.get("lotSize"))
            if lot <= 0:
                lot = self.lot_cache.get(symbol, 1)
            else:
                self.lot_cache[symbol] = lot

            quotes.append(FutureQuote(
                symbol        = symbol,
                expiry        = expiry,
                last_price    = price,
                daily_volume  = self._i(meta.get("totalTradedVolume")),
                lot_size      = lot,
                open_interest = self._i(meta.get("openInterest")),
                high          = self._f(meta.get("highPrice"), price),
                low           = self._f(meta.get("lowPrice"),  price),
                prev_close    = self._f(meta.get("prevClose"),  price),
            ))

        quotes.sort(key=lambda q: self._parse_expiry(q.expiry))
        return quotes

    # ---- volume filter ------------------------------------------
    def _vol_ok(self, far: FutureQuote, lot_size: int) -> bool:
        """
        Estimate 1-min volume = daily_volume / 375 trading minutes.
        Require: 1-min vol >= VOLUME_FILTER_MULT * lot_size.
        """
        if far.daily_volume <= 0:
            return False
        return (far.daily_volume / 375.0) >= (VOLUME_FILTER_MULT * lot_size)

    # ---- full market scan ---------------------------------------
    def scan_all(self, available_capital: float) -> List[SpreadOpp]:
        """
        Scan every F&O symbol and compute calendar spreads.

        Entry filter:
          Theoretical max profit (if spread fully collapses to 0):
            max_profit = |spread_abs| * lot_size
            max_profit_pct = max_profit / margin_required * 100

          can_trade = True only when:
            (1) margin_required <= available_capital
            (2) far-month 1-min estimated vol >= 150 * lot_size
            (3) max_profit_pct >= MIN_MAX_PROFIT_PCT  (10%)

        Sorted by max_profit_pct descending.
        """
        all_symbols = (
            [(s, True)  for s in FNO_INDICES] +
            [(s, False) for s in FNO_STOCKS]
        )
        opps: List[SpreadOpp] = []

        for symbol, is_index in all_symbols:
            try:
                chain = self.fetch_chain(symbol)
                if len(chain) < 2:
                    continue

                near = chain[0]
                far  = chain[1]
                lot  = near.lot_size

                spread_abs = far.last_price - near.last_price
                spread_pct = (spread_abs / near.last_price) * 100

                # Skip zero / stale spreads
                if abs(spread_abs) < 0.01:
                    continue

                contract_val    = near.last_price * lot
                margin_required = contract_val * CALENDAR_MARGIN_PCT

                if margin_required <= 0:
                    continue

                # Theoretical max profit on margin
                max_profit_pct = (abs(spread_abs) * lot / margin_required) * 100

                margin_ok = margin_required <= available_capital
                volume_ok = self._vol_ok(far, lot)
                profit_ok = max_profit_pct >= MIN_MAX_PROFIT_PCT
                can_trade = margin_ok and volume_ok and profit_ok

                opps.append(SpreadOpp(
                    symbol          = symbol,
                    is_index        = is_index,
                    near            = near,
                    far             = far,
                    spread_abs      = spread_abs,
                    spread_pct      = spread_pct,
                    max_profit_pct  = max_profit_pct,
                    margin_required = margin_required,
                    volume_ok       = volume_ok,
                    can_trade       = can_trade,
                ))

                time.sleep(0.15)   # polite rate-limiting

            except Exception:
                continue

        opps.sort(key=lambda o: o.max_profit_pct, reverse=True)
        return opps


# ================================================================
#  PAPER PORTFOLIO
# ================================================================
class Portfolio:
    """Manages paper capital, open positions, and trade history."""

    def __init__(self, initial_capital: float):
        self.initial_capital = initial_capital
        self.available_cash  = initial_capital
        self.realised_pnl    = 0.0
        self.total_fees_paid = 0.0
        self.active_trades:  List[Trade] = []
        self.closed_trades:  List[Trade] = []
        self._counter        = 0

    @property
    def margin_in_use(self) -> float:
        return sum(t.margin_used for t in self.active_trades)

    @property
    def unrealised_pnl(self) -> float:
        return sum(t.net_pnl for t in self.active_trades)

    @property
    def total_equity(self) -> float:
        return self.initial_capital + self.realised_pnl + self.unrealised_pnl

    @property
    def total_return_pct(self) -> float:
        return (
            (self.total_equity - self.initial_capital)
            / self.initial_capital * 100
        )

    # ---- trade lifecycle ----------------------------------------
    def can_open(self, opp: SpreadOpp) -> bool:
        already_in = any(t.symbol == opp.symbol for t in self.active_trades)
        return (
            opp.can_trade
            and not already_in
            and opp.margin_required <= self.available_cash
            and len(self.active_trades) < MAX_OPEN_POSITIONS
        )

    def open_trade(self, opp: SpreadOpp) -> Optional[Trade]:
        if not self.can_open(opp):
            return None

        self._counter += 1
        lots   = max(1, int(self.available_cash / opp.margin_required))
        lots   = min(lots, 3)   # cap at 3 lots per position
        margin = opp.margin_required * lots
        fees   = calc_costs(opp.near.last_price, opp.lot_size, lots)

        direction = (
            "BUY_NEAR_SELL_FAR"
            if opp.spread_pct >= 0
            else "SELL_NEAR_BUY_FAR"
        )

        trade = Trade(
            trade_id       = "T{:04d}".format(self._counter),
            symbol         = opp.symbol,
            is_index       = opp.is_index,
            direction      = direction,
            near_expiry    = opp.near.expiry,
            far_expiry     = opp.far.expiry,
            near_entry     = opp.near.last_price,
            far_entry      = opp.far.last_price,
            lot_size       = opp.lot_size,
            lots           = lots,
            entry_spread   = opp.spread_abs,
            entry_time     = datetime.datetime.now(),
            margin_used    = margin,
            near_now       = opp.near.last_price,
            far_now        = opp.far.last_price,
            current_spread = opp.spread_abs,
        )

        self.available_cash  -= margin
        self.total_fees_paid += fees
        self.active_trades.append(trade)
        return trade

    def update_trade(self, trade: Trade,
                     near_price: float, far_price: float) -> bool:
        """Update live PnL. Returns True when profit target is hit."""
        qty = trade.lot_size * trade.lots

        trade.near_now       = near_price
        trade.far_now        = far_price
        trade.current_spread = far_price - near_price

        if trade.direction == "BUY_NEAR_SELL_FAR":
            gross = ((near_price - trade.near_entry) +
                     (trade.far_entry - far_price)) * qty
        else:
            gross = ((trade.near_entry - near_price) +
                     (far_price - trade.far_entry)) * qty

        fees = calc_costs(near_price, trade.lot_size, trade.lots)
        trade.gross_pnl   = gross
        trade.net_pnl     = gross - fees
        trade.net_pnl_pct = (trade.net_pnl / trade.margin_used) * 100

        return trade.net_pnl_pct >= PROFIT_TARGET_PCT

    def close_trade(self, trade: Trade,
                    near_price: float, far_price: float,
                    reason: str = "PROFIT_TARGET") -> None:
        """Realise PnL and free margin."""
        qty  = trade.lot_size * trade.lots
        fees = calc_costs(near_price, trade.lot_size, trade.lots)

        if trade.direction == "BUY_NEAR_SELL_FAR":
            gross = ((near_price - trade.near_entry) +
                     (trade.far_entry - far_price)) * qty
        else:
            gross = ((trade.near_entry - near_price) +
                     (far_price - trade.far_entry)) * qty

        net = gross - fees

        trade.is_closed      = True
        trade.exit_time      = datetime.datetime.now()
        trade.exit_near      = near_price
        trade.exit_far       = far_price
        trade.final_net_pnl  = net
        trade.gross_pnl      = gross
        trade.net_pnl        = net
        trade.net_pnl_pct    = (net / trade.margin_used) * 100
        trade.exit_reason    = reason

        self.available_cash  += trade.margin_used + net
        self.realised_pnl    += net
        self.total_fees_paid += fees

        self.active_trades.remove(trade)
        self.closed_trades.append(trade)


# ================================================================
#  DISPLAY  (Rich terminal UI)
# ================================================================
class UI:
    """All terminal output lives here."""

    TITLE    = "[bold cyan]CALENDAR SPREAD ARBITRAGE BOT[/bold cyan]"
    SUBTITLE = "[dim]NSE India | Paper Trading | urllib (no requests)[/dim]"

    def __init__(self):
        self.con = Console()

    # ---- startup banner ----------------------------------------
    def banner(self):
        self.con.print()
        self.con.print(Panel.fit(
            "{}\n{}\n\n"
            "[yellow]Capital:[/yellow] Rs.{:,}  |  "
            "[yellow]Exit target:[/yellow] +{}% net on margin  |  "
            "[yellow]Entry filter:[/yellow] max-profit >= {}% on margin  |  "
            "[yellow]Interval:[/yellow] {}s  |  "
            "[yellow]Vol filter:[/yellow] 1-min >= {}x".format(
                self.TITLE, self.SUBTITLE,
                INITIAL_CAPITAL, PROFIT_TARGET_PCT,
                MIN_MAX_PROFIT_PCT, SCAN_INTERVAL_SEC, VOLUME_FILTER_MULT,
            ),
            border_style="bright_cyan",
            padding=(1, 4),
        ))
        self.con.print()

    # ---- portfolio header --------------------------------------
    def print_header(self, portfolio: Portfolio, scan: int):
        ts       = datetime.datetime.now().strftime("%Y-%m-%d  %H:%M:%S")
        pnl      = portfolio.realised_pnl + portfolio.unrealised_pnl
        sign     = "+" if pnl >= 0 else ""
        col      = "green" if pnl >= 0 else "red"
        ret      = portfolio.total_return_pct
        ret_sign = "+" if ret >= 0 else ""

        body = (
            "[dim]Time: {}[/dim]  |  [dim]Scan #{}[/dim]\n\n"
            "[bold]Capital[/bold]    Rs.{:>10,.0f}   "
            "[bold]Equity[/bold]  Rs.{:>10,.2f}   "
            "[bold {col}]Net P&L[/bold {col}] "
            "[bold {col}]{sign}Rs.{pnl:.2f}  ({rsign}{ret:.2f}%)[/bold {col}]\n"
            "[dim]Margin in use[/dim] Rs.{:>10,.0f}   "
            "[dim]Free cash[/dim]    Rs.{:>10,.0f}   "
            "[dim]Open positions[/dim] {}   [dim]Closed[/dim] {}"
        ).format(
            ts, scan,
            portfolio.initial_capital,
            portfolio.total_equity,
            portfolio.margin_in_use,
            portfolio.available_cash,
            len(portfolio.active_trades),
            len(portfolio.closed_trades),
            col=col, sign=sign,
            pnl=abs(pnl), rsign=ret_sign, ret=abs(ret),
        )
        self.con.print(Panel(body, border_style="cyan",
                             style="on grey15", title=self.TITLE))

    # ---- top opportunities table --------------------------------
    def print_opps_table(self, opps: List[SpreadOpp],
                         portfolio: Portfolio):
        if not opps:
            self.con.print(Panel(
                "[yellow]No spread data -- "
                "market may be closed or NSE API unreachable.[/yellow]",
                title="Top Spread Opportunities",
                border_style="yellow",
            ))
            return

        t = Table(
            title=(
                "Top {} Calendar Spread Opportunities  "
                "[dim](sorted by max-profit % on margin | "
                "entry filter >= {}%)[/dim]".format(TOP_N, MIN_MAX_PROFIT_PCT)
            ),
            box=box.ROUNDED,
            border_style="bright_cyan",
            header_style="bold magenta",
            show_lines=True,
            pad_edge=False,
        )
        t.add_column("#",           style="dim",        width=3,  justify="right")
        t.add_column("Symbol",      style="bold white",  width=14)
        t.add_column("Type",        style="cyan",       width=5)
        t.add_column("Near Rs.",    style="yellow",     width=14, justify="right")
        t.add_column("Near Exp",    style="dim",        width=12)
        t.add_column("Far Rs.",     style="yellow",     width=14, justify="right")
        t.add_column("Far Exp",     style="dim",        width=12)
        t.add_column("Spread Rs.",  justify="right",    width=12)
        t.add_column("Spread %",    justify="right",    width=10)
        t.add_column("MaxProfit %", justify="right",    width=12)
        t.add_column("Margin Rs.",  style="dim",        width=12, justify="right")
        t.add_column("Vol OK",      width=7,            justify="center")
        t.add_column("Trade?",      width=8,            justify="center")

        active_syms = {tr.symbol for tr in portfolio.active_trades}

        for i, o in enumerate(opps[:TOP_N], 1):
            sc      = "green" if o.spread_pct >= 0 else "red"
            vol_str = "[green]YES[/green]" if o.volume_ok else "[red]NO[/red]"
            if o.symbol in active_syms:
                can_str = "[dim]-- OPEN[/dim]"
            elif o.can_trade:
                can_str = "[green]YES[/green]"
            else:
                can_str = "[red]NO[/red]"
            typ_str = "[blue]IDX[/blue]" if o.is_index else "[white]STK[/white]"
            mp_col  = "bold green" if o.max_profit_pct >= MIN_MAX_PROFIT_PCT else "dim yellow"

            t.add_row(
                str(i),
                o.symbol,
                typ_str,
                "Rs.{:>9,.2f}".format(o.near.last_price),
                o.near.expiry,
                "Rs.{:>9,.2f}".format(o.far.last_price),
                o.far.expiry,
                "[{sc}]{:>+10.2f}[/{sc}]".format(o.spread_abs, sc=sc),
                "[bold {sc}]{:>+7.2f}%[/bold {sc}]".format(o.spread_pct, sc=sc),
                "[{mc}]{:>+7.1f}%[/{mc}]".format(o.max_profit_pct, mc=mp_col),
                "Rs.{:>8,.0f}".format(o.margin_required),
                vol_str,
                can_str,
            )

        self.con.print(t)

    # ---- active positions table ---------------------------------
    def print_positions(self, portfolio: Portfolio):
        if not portfolio.active_trades:
            self.con.print(Panel(
                "[dim italic]No active positions "
                "-- scanning for entry opportunity...[/dim italic]",
                title="Active Positions",
                border_style="yellow",
            ))
            return

        t = Table(
            box=box.SIMPLE_HEAVY,
            header_style="bold yellow",
            show_lines=True,
            pad_edge=False,
            title="Active Positions",
        )
        t.add_column("ID",           style="dim",   width=7)
        t.add_column("Symbol",       style="bold",  width=14)
        t.add_column("Direction",                   width=22)
        t.add_column("Entry Spread", justify="right", width=14)
        t.add_column("Now Spread",   justify="right", width=13)
        t.add_column("Delta",        justify="right", width=10)
        t.add_column("Gross P&L",    justify="right", width=13)
        t.add_column("Net P&L %",    justify="right", width=11)
        t.add_column("Lots",         justify="right", width=5)
        t.add_column("Elapsed",                      width=9)

        for tr in portfolio.active_trades:
            elapsed = datetime.datetime.now() - tr.entry_time
            mins    = int(elapsed.total_seconds() / 60)
            pc      = "green" if tr.net_pnl     >= 0 else "red"
            ppct_c  = "green" if tr.net_pnl_pct >= 0 else "red"
            delta   = tr.current_spread - tr.entry_spread
            dc      = "green" if delta <= 0 else "red"
            sign    = "+" if tr.net_pnl >= 0 else ""

            if tr.direction == "BUY_NEAR_SELL_FAR":
                dir_str = "[green]Buy NEAR / Sell FAR[/green]"
            else:
                dir_str = "[red]Sell NEAR / Buy FAR[/red]"

            t.add_row(
                tr.trade_id,
                tr.symbol,
                dir_str,
                "Rs.{:>+,.2f}".format(tr.entry_spread),
                "Rs.{:>+,.2f}".format(tr.current_spread),
                "[{dc}]{:>+,.2f}[/{dc}]".format(delta, dc=dc),
                "[{pc}]{sign}Rs.{:,.2f}[/{pc}]".format(
                    abs(tr.gross_pnl), pc=pc, sign=sign),
                "[bold {pc}]{sign}{:.2f}%[/bold {pc}]".format(
                    abs(tr.net_pnl_pct), pc=ppct_c, sign=sign),
                str(tr.lots),
                "{}m".format(mins),
            )

        self.con.print(Panel(t, border_style="yellow"))

    # ---- closed trades log --------------------------------------
    def print_closed(self, portfolio: Portfolio):
        if not portfolio.closed_trades:
            return

        count = min(5, len(portfolio.closed_trades))
        t = Table(
            box=box.SIMPLE,
            header_style="bold",
            title="Closed Trades (last {})".format(count),
        )
        t.add_column("ID",         style="dim")
        t.add_column("Symbol",     style="bold")
        t.add_column("Dir",        width=8)
        t.add_column("Lots",       justify="right")
        t.add_column("Entry Sprd", justify="right")
        t.add_column("Net P&L",    justify="right")
        t.add_column("Return %",   justify="right")
        t.add_column("Duration",   width=9)
        t.add_column("Reason",     style="dim")

        for tr in list(reversed(portfolio.closed_trades))[:5]:
            dur      = tr.exit_time - tr.entry_time
            mins     = int(dur.total_seconds() / 60)
            pc       = "green" if tr.final_net_pnl >= 0 else "red"
            rpct     = (tr.final_net_pnl / tr.margin_used) * 100
            sign     = "+" if tr.final_net_pnl >= 0 else ""
            dir_abbr = "BN/SF" if tr.direction == "BUY_NEAR_SELL_FAR" else "SN/BF"

            t.add_row(
                tr.trade_id,
                tr.symbol,
                dir_abbr,
                str(tr.lots),
                "Rs.{:+,.2f}".format(tr.entry_spread),
                "[bold {pc}]{sign}Rs.{:,.2f}[/bold {pc}]".format(
                    abs(tr.final_net_pnl), pc=pc, sign=sign),
                "[{pc}]{sign}{:.2f}%[/{pc}]".format(abs(rpct), pc=pc, sign=sign),
                "{}m".format(mins),
                tr.exit_reason,
            )

        self.con.print(Panel(t, border_style="dim green"))

    # ---- trade opened notification ------------------------------
    def print_trade_open(self, trade: Trade):
        if trade.direction == "BUY_NEAR_SELL_FAR":
            dir_str = "[green]BUY NEAR + SELL FAR[/green]"
        else:
            dir_str = "[red]SELL NEAR + BUY FAR[/red]"

        self.con.print(Panel(
            "[bold green]NEW POSITION OPENED[/bold green]   {}\n\n"
            "  Symbol    : [bold]{}[/bold]  "
            "({} units/lot x {} lot(s))\n"
            "  Direction : {}\n"
            "  Near ({}) : [yellow]Rs.{:,.2f}[/yellow]\n"
            "  Far  ({}) : [yellow]Rs.{:,.2f}[/yellow]\n"
            "  Entry Spread : [cyan]Rs.{:+,.2f}[/cyan]   "
            "Margin : [cyan]Rs.{:,.0f}[/cyan]\n"
            "  Exit target : [bold green]+{}% net P&L on margin[/bold green]".format(
                trade.trade_id,
                trade.symbol, trade.lot_size, trade.lots,
                dir_str,
                trade.near_expiry, trade.near_entry,
                trade.far_expiry,  trade.far_entry,
                trade.entry_spread, trade.margin_used,
                PROFIT_TARGET_PCT,
            ),
            border_style="green",
        ))

    # ---- trade closed notification ------------------------------
    def print_trade_close(self, trade: Trade):
        pc   = "green" if trade.final_net_pnl >= 0 else "red"
        sign = "+" if trade.final_net_pnl >= 0 else ""
        rpct = (trade.final_net_pnl / trade.margin_used) * 100

        self.con.print(Panel(
            "[bold {pc}]POSITION CLOSED[/bold {pc}]   {} -- {}\n\n"
            "  Symbol : [bold]{}[/bold]\n"
            "  Entry  : near Rs.{:,.2f}  far Rs.{:,.2f}\n"
            "  Exit   : near Rs.{:,.2f}  far Rs.{:,.2f}\n"
            "  Gross P&L : Rs.{:+,.2f}   "
            "Net P&L : [bold {pc}]{sign}Rs.{:,.2f}  ({sign}{:.2f}%)[/bold {pc}]".format(
                trade.trade_id, trade.exit_reason,
                trade.symbol,
                trade.near_entry, trade.far_entry,
                trade.exit_near,  trade.exit_far,
                trade.gross_pnl,
                abs(trade.final_net_pnl), abs(rpct),
                pc=pc, sign=sign,
            ),
            border_style=pc,
        ))

    # ---- footer ------------------------------------------------
    def print_footer(self):
        self.con.print(
            "[dim]   Next scan in {}s  |  "
            "Strategy: Calendar Spread Arbitrage  |  "
            "Exit at +{}% net  |  Ctrl-C to quit[/dim]".format(
                SCAN_INTERVAL_SEC, PROFIT_TARGET_PCT,
            )
        )
        self.con.rule(style="dim")


# ================================================================
#  MAIN BOT
# ================================================================
class CalendarSpreadBot:
    """Orchestrates scanning, position management, and display."""

    def __init__(self):
        self.ui        = UI()
        self.session   = NSESession()
        self.fetcher   = NSEFetcher(self.session)
        self.portfolio = Portfolio(INITIAL_CAPITAL)
        self.scan_num  = 0
        self.running   = True

    # ---- one full scan cycle -----------------------------------
    def run_scan(self):
        self.scan_num += 1
        con = self.ui.con

        con.clear()
        con.print("[dim]Running scan #{}...[/dim]".format(self.scan_num))

        # 1. Fetch all spread opportunities
        opps = self.fetcher.scan_all(self.portfolio.available_cash)

        # 2. Update active trades; collect exits
        exits: List[Tuple[Trade, float, float]] = []
        for trade in list(self.portfolio.active_trades):
            matched = next(
                (o for o in opps if o.symbol == trade.symbol), None
            )
            if matched:
                hit = self.portfolio.update_trade(
                    trade,
                    matched.near.last_price,
                    matched.far.last_price,
                )
                if hit:
                    exits.append((
                        trade,
                        matched.near.last_price,
                        matched.far.last_price,
                    ))

        # Close trades that hit profit target
        for trade, np_, fp_ in exits:
            self.portfolio.close_trade(trade, np_, fp_, "PROFIT_TARGET_10pct")
            self.ui.print_trade_close(trade)

        # 3. Open new trade if room available
        if len(self.portfolio.active_trades) < MAX_OPEN_POSITIONS and opps:
            active_syms = {t.symbol for t in self.portfolio.active_trades}
            for opp in opps:
                if (
                    opp.can_trade
                    and opp.symbol not in active_syms
                    and opp.margin_required <= self.portfolio.available_cash
                ):
                    new_trade = self.portfolio.open_trade(opp)
                    if new_trade:
                        self.ui.print_trade_open(new_trade)
                        break  # one new trade per scan

        # 4. Render full dashboard
        con.clear()
        self.ui.print_header(self.portfolio, self.scan_num)
        self.ui.print_opps_table(opps, self.portfolio)
        self.ui.print_positions(self.portfolio)
        self.ui.print_closed(self.portfolio)
        self.ui.print_footer()

    # ---- main loop ---------------------------------------------
    def run(self):
        self.ui.banner()
        self.ui.con.print(
            "[dim]Initialising NSE session (urllib) -- fetching cookies...[/dim]"
        )
        time.sleep(2)

        while self.running:
            try:
                self.run_scan()
                self.ui.con.print(
                    "\n[dim]Sleeping {}s until next scan...[/dim]".format(
                        SCAN_INTERVAL_SEC)
                )
                time.sleep(SCAN_INTERVAL_SEC)

            except KeyboardInterrupt:
                self._shutdown()
                break
            except Exception as exc:
                self.ui.con.print("[red]Error: {}[/red]".format(exc))
                time.sleep(10)

    # ---- shutdown summary --------------------------------------
    def _shutdown(self):
        con       = self.ui.con
        p         = self.portfolio
        total_pnl = p.realised_pnl + p.unrealised_pnl
        ret_pct   = p.total_return_pct
        sign      = "+" if total_pnl >= 0 else ""
        pc        = "green" if total_pnl >= 0 else "red"

        con.print("\n\n[yellow]Bot stopped by user (Ctrl-C).[/yellow]\n")
        con.rule("[bold cyan]FINAL SUMMARY[/bold cyan]")
        con.print("  Total scans   : {}".format(self.scan_num))
        con.print("  Trades opened : {}".format(p._counter))
        con.print("  Trades closed : {}".format(len(p.closed_trades)))
        con.print("  Fees paid     : Rs.{:,.2f}".format(p.total_fees_paid))
        con.print(
            "  Net P&L       : [bold {pc}]{sign}Rs.{:,.2f}  "
            "({sign}{:.2f}%)[/bold {pc}]".format(
                abs(total_pnl), abs(ret_pct),
                pc=pc, sign=sign,
            )
        )
        con.print("  Final equity  : Rs.{:,.2f}".format(p.total_equity))
        con.rule()


# ================================================================
#  ENTRY POINT
# ================================================================
if __name__ == "__main__":
    print()
    print("=" * 66)
    print("  CALENDAR SPREAD ARBITRAGE BOT  --  NSE India  --  Paper Trade")
    print("  HTTP layer: Python stdlib urllib  (NO requests library)")
    print("=" * 66)
    print()
    print("  Only ONE install needed:  pip install rich")
    print()

    bot = CalendarSpreadBot()
    bot.run()
