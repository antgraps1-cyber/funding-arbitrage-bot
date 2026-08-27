"""
════════════════════════════════════════════════════════════════════════════════
  FUNDING RATE ARBITRAGE BOT  v7  —  Full-Scan Edition
════════════════════════════════════════════════════════════════════════════════

  CHANGES vs v6:
  ─────────────
  • Scans ALL common coins every cycle (no early-exit when "neither fires
    this hour") — previously most coins were silently skipped.
  • Top-10 display sorted by raw_diff (absolute funding rate difference)
    regardless of timing — you always see the real best opportunities.
  • Scan interval fixed to 60 seconds (idle AND active).
  • Markets loaded ONCE at startup, not re-fetched every scan cycle.
  • Added raw_diff to Opportunity dataclass for proper sorting.
  • Cleaner top-10 table: shows raw rates, which exchange fires, raw diff.
  • Better error handling if one exchange returns partial/empty data.
  • Retry logic on symbol-load failure.

  INSTALL:
    pip install ccxt

  PROXY (if Binance is blocked in your region):
    Option 1 — environment variable:   set HTTP_PROXY=http://user:pass@host:port
    Option 2 — edit CFG below:         "PROXY": "http://user:pass@host:port"

  STRATEGY:
  ─────────
  • fires_this_hour:  nextFundingTime <= next x:00:00 UTC + HOUR_GRACE_SEC
  • eff_rate:         real rate if fires this hour, else 0.0
  • raw_diff:         |rate_binance - rate_bybit|  (always, ignores timing)
  • adj_diff:         |eff_rate_binance - eff_rate_bybit|  (timing-aware)
  • Score:            max(net_pct,0) * freq_bonus * timing_bonus - spread_penalty
  • One best trade at a time, exit after funding collected

════════════════════════════════════════════════════════════════════════════════
"""

import asyncio
import os
import csv
import uuid
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import ccxt.async_support as ccxt_async
import logging
from logging.handlers import RotatingFileHandler

# ══════════════════════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════════════════════
os.makedirs("logs", exist_ok=True)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh  = RotatingFileHandler("logs/bot.log", maxBytes=5 * 1024 * 1024,
                            backupCount=3, encoding="utf-8")
_fh.setFormatter(_fmt)


class SafeStreamHandler(logging.StreamHandler):
    _MAP = {"→": "->", "✓": "[OK]", "✗": "[NO]", "⚠": "[!]",
            "★": "*",  "🚀": ">>>", "💰": "$$$", "📊": "[=]"}

    def emit(self, record):
        try:
            msg = self.format(record)
            for a, b in self._MAP.items():
                msg = msg.replace(a, b)
            self.stream.write(msg + self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)


_ch = SafeStreamHandler()
_ch.setFormatter(_fmt)
logger = logging.getLogger("FundingBot")
logger.setLevel(logging.INFO)
logger.addHandler(_fh)
logger.addHandler(_ch)

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
CFG: Dict = {
    # ── Strategy timing ───────────────────────────────────────────────────────
    "HOUR_GRACE_SEC":          120,    # tolerance past x:00 for "fires this hour"
    "ENTRY_WINDOW_MIN":          8,    # open trade up to 8 min before funding
    "MIN_TIME_TO_FUNDING_MIN":   2.0,  # abort entry if < 2 min to snapshot
    "EXIT_DELAY_SEC":           30,    # wait 30 s after later funding fires

    # ── Fees & costs ──────────────────────────────────────────────────────────
    "TAKER_FEE":               0.0004,  # 0.04% per side
    "SLIPPAGE":                0.0002,  # 0.02% simulated per side

    # ── Price gap → leverage ──────────────────────────────────────────────────
    "PRICE_GAP_SKIP":          0.006,   # > 0.6%  → hard skip
    "PRICE_GAP_LOW_LEV":       0.003,   # > 0.3%  → 2.5× leverage
    "LEV_TIGHT":               4.0,     # gap <= 0.3%
    "LEV_WIDE":                2.5,     # gap 0.3–0.6%
    "MAX_LEVERAGE":            4.0,

    # ── Filters ───────────────────────────────────────────────────────────────
    "MIN_VOLUME_USDT":         2_000_000,

    # ── Scoring ───────────────────────────────────────────────────────────────
    "FREQ_BONUS":              {60: 1.50, 240: 1.15, 480: 1.00},
    "FREQ_BONUS_DEFAULT":      1.00,
    "TIMING_ALPHA":            3.0,

    # ── Capital ───────────────────────────────────────────────────────────────
    "INITIAL_BALANCE":         27.48,   # per exchange (paper trading)
    "CAPITAL_PCT":             0.90,
    "BUFFER_RATE":             0.0005,

    # ── Network ───────────────────────────────────────────────────────────────
    "REQUEST_TIMEOUT":         30_000,  # ms (used by CCXT)
    "CCXT_RATE_LIMIT":         True,

    # ── Bot loop ──────────────────────────────────────────────────────────────
    # v7: fixed to 60 s for both states so you always see fresh data each minute
    "SCAN_INTERVAL_IDLE":      60,
    "SCAN_INTERVAL_ACTIVE":    60,

    # ── Persistence ───────────────────────────────────────────────────────────
    "CSV_FILE":                "trades.csv",

    # ── Proxy ─────────────────────────────────────────────────────────────────
    # Set to None or "http://user:pass@host:port"
    # Can also be set via HTTP_PROXY / HTTPS_PROXY environment variable.
    "PROXY":                   None,

    # ── Display ───────────────────────────────────────────────────────────────
    "TOP_N":                   10,      # rows in the top-N table
}

ROUND_TRIP_COST = 4 * CFG["TAKER_FEE"] + 2 * CFG["SLIPPAGE"]   # 0.0020
W = 170   # console line width


# ══════════════════════════════════════════════════════════════════════════════
#  SYMBOL HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def to_display(ccxt_sym: str) -> str:
    """BTC/USDT:USDT  →  BTCUSDT"""
    try:
        base, rest = ccxt_sym.split("/", 1)
        quote = rest.split(":")[0]
        return base + quote
    except Exception:
        return ccxt_sym


# ══════════════════════════════════════════════════════════════════════════════
#  DATA CLASSES
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class ExchangeData:
    name:         str
    symbol:       str
    raw_rate:     Optional[float]
    next_funding: Optional[datetime]
    interval_min: int
    price:        Optional[float]
    volume_24h:   Optional[float]


@dataclass
class Opportunity:
    symbol:               str
    ex_a:                 ExchangeData
    ex_b:                 ExchangeData
    # Raw rates (used for top-10 ranking, always set)
    raw_rate_a:           float
    raw_rate_b:           float
    raw_diff:             float          # |raw_rate_a - raw_rate_b|  <- KEY SORT FIELD
    # Effective rates (timing-aware, used for trading decisions)
    eff_rate_a:           float
    eff_rate_b:           float
    fires_this_hour_a:    bool
    fires_this_hour_b:    bool
    short_exchange:       str
    long_exchange:        str
    adj_diff:             float
    net_pct:              float
    price_gap_pct:        float
    target_funding_time:  Optional[datetime]
    later_funding_time:   Optional[datetime]
    time_to_funding_min:  float
    entry_open_time:      Optional[datetime]
    planned_exit_time:    Optional[datetime]
    leverage:             float
    freq_bonus:           float
    timing_bonus:         float
    score:                float
    eligible:             bool
    skip_reason:          str = ""


@dataclass
class TradeRecord:
    trade_id:             str
    symbol:               str
    long_exchange:        str
    short_exchange:       str
    entry_time:           datetime
    exit_time:            Optional[datetime]
    entry_price_long:     float
    entry_price_short:    float
    exit_price_long:      Optional[float]
    exit_price_short:     Optional[float]
    notional:             float
    leverage:             float
    eff_rate_long:        float
    eff_rate_short:       float
    adj_diff:             float
    net_pct:              float
    target_funding_time:  datetime
    later_funding_time:   datetime
    planned_exit_time:    datetime
    pos_id_long:          str
    pos_id_short:         str
    entry_fees:           float = 0.0
    pnl_long:             Optional[float] = None
    pnl_short:            Optional[float] = None
    funding_pnl_long:     float = 0.0
    funding_pnl_short:    float = 0.0
    funding_collected:    bool  = False
    exit_fees:            float = 0.0
    total_pnl:            Optional[float] = None
    status:               str = "OPEN"


# ══════════════════════════════════════════════════════════════════════════════
#  CCXT EXCHANGE
# ══════════════════════════════════════════════════════════════════════════════
class CCXTExchange:
    def __init__(self, name: str):
        self.name            = name
        self.balance         = CFG["INITIAL_BALANCE"]
        self.positions:      Dict[str, dict] = {}
        self._exchange:      Optional[ccxt_async.Exchange] = None
        self._interval_map:  Dict[str, int] = {}

    def _make_exchange(self) -> ccxt_async.Exchange:
        proxy = (CFG.get("PROXY")
                 or os.environ.get("HTTPS_PROXY")
                 or os.environ.get("HTTP_PROXY"))
        base = {
            "enableRateLimit": CFG["CCXT_RATE_LIMIT"],
            "timeout":         CFG["REQUEST_TIMEOUT"],
        }
        if proxy:
            base["aiohttp_proxy"] = proxy
            base["proxies"]       = {"http": proxy, "https": proxy}

        if self.name == "binance":
            return ccxt_async.binanceusdm({
                **base,
                "options": {
                    "defaultType":             "swap",
                    "adjustForTimeDifference": True,
                },
            })
        else:
            return ccxt_async.bybit({
                **base,
                "options": {
                    "defaultType": "swap",
                    "category":    "linear",
                },
            })

    async def _ex(self) -> ccxt_async.Exchange:
        if self._exchange is None:
            self._exchange = self._make_exchange()
        return self._exchange

    # ── Symbol loading (called ONCE at startup) ───────────────────────────────

    async def load_symbols(self, max_retries: int = 3) -> Dict[str, int]:
        """
        Returns {ccxt_symbol: interval_minutes}.
        Retries up to max_retries times on network error.
        """
        ex = await self._ex()
        markets = None

        for attempt in range(1, max_retries + 1):
            try:
                # reload=True only on first attempt; False on retries to use cache
                markets = await ex.load_markets(reload=(attempt == 1))
                break
            except ccxt_async.NetworkError as e:
                logger.warning(f"[{self.name}] load_markets attempt {attempt}/{max_retries}: {e}")
                if attempt == max_retries:
                    logger.error(f"[{self.name}] load_markets failed after {max_retries} attempts.")
                    _print_network_help()
                    return {}
                await asyncio.sleep(5 * attempt)
            except Exception as e:
                logger.error(f"[{self.name}] load_markets unexpected error: {e}")
                return {}

        if not markets:
            return {}

        bad    = {"UP", "DOWN", "BEAR", "BULL", "3L", "3S"}
        result = {}

        for sym, mkt in markets.items():
            if not (mkt.get("active")
                    and mkt.get("quote") == "USDT"
                    and mkt.get("linear")
                    and mkt.get("type") in ("swap", "future")):
                continue

            base = mkt.get("base", "")
            if any(x in base for x in bad):
                continue

            info = mkt.get("info", {})
            try:
                if self.name == "binance":
                    hrs      = int(float(info.get("fundingIntervalHours", 8)))
                    interval = hrs * 60
                else:
                    interval = int(info.get("fundingInterval", 480))
            except (TypeError, ValueError):
                interval = 480

            result[sym]               = interval
            self._interval_map[sym]   = interval

        logger.info(f"[{self.name}] {len(result)} symbols loaded")
        return result

    # ── Batch market data ─────────────────────────────────────────────────────

    async def fetch_market_data(self) -> Tuple[Dict, Dict]:
        """
        2 API calls per exchange:
          fetch_funding_rates() — all funding rates in one request
          fetch_tickers()       — all tickers (price + volume) in one request
        """
        ex   = await self._ex()
        frs  = {}
        tkrs = {}

        try:
            frs = await ex.fetch_funding_rates()
        except ccxt_async.NetworkError as e:
            logger.error(f"[{self.name}] fetch_funding_rates network error: {e}")
            _print_network_help()
        except Exception as e:
            logger.error(f"[{self.name}] fetch_funding_rates: {type(e).__name__}: {e}")

        try:
            tkrs = await ex.fetch_tickers()
        except ccxt_async.NetworkError as e:
            logger.error(f"[{self.name}] fetch_tickers network error: {e}")
        except Exception as e:
            logger.error(f"[{self.name}] fetch_tickers: {type(e).__name__}: {e}")

        return frs, tkrs

    def parse_exchange_data(self, symbol: str,
                            fr: Optional[dict],
                            tk: Optional[dict]) -> ExchangeData:
        rate, nft, price, vol = None, None, None, None

        if fr:
            try:
                raw = fr.get("fundingRate")
                if raw is not None:
                    rate = float(raw)
            except (TypeError, ValueError):
                pass

            ts = fr.get("fundingTimestamp")
            if ts:
                try:
                    dt  = datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc)
                    now = datetime.now(timezone.utc)
                    if 2020 < dt.year < 2035 and dt > now - timedelta(minutes=30):
                        nft = dt
                except (TypeError, ValueError, OSError):
                    pass

        if tk:
            try:
                price = float(tk.get("last") or tk.get("close") or 0) or None
            except (TypeError, ValueError):
                price = None
            try:
                vol = float(tk.get("quoteVolume") or 0) or None
                if not vol and price:
                    base_vol = float(tk.get("baseVolume") or 0)
                    vol = base_vol * price if base_vol else None
            except (TypeError, ValueError):
                vol = None

        return ExchangeData(
            name         = self.name,
            symbol       = symbol,
            raw_rate     = rate,
            next_funding = nft,
            interval_min = self._interval_map.get(symbol, 480),
            price        = price,
            volume_24h   = vol,
        )

    async def get_exit_price(self, symbol: str, fallback: float) -> float:
        ex = await self._ex()
        try:
            tk = await ex.fetch_ticker(symbol)
            p  = float(tk.get("last") or tk.get("close") or 0)
            return p if p > 0 else fallback
        except Exception as e:
            logger.warning(f"[{self.name}] get_exit_price({to_display(symbol)}): {e}")
            return fallback

    # ── Paper position management ─────────────────────────────────────────────

    def open_position(self, symbol: str, side: str, price: float,
                      notional: float, leverage: float) -> Optional[str]:
        margin   = notional / leverage
        fee      = notional * CFG["TAKER_FEE"]
        slip     = notional * CFG["SLIPPAGE"]
        buf      = notional * CFG["BUFFER_RATE"]
        required = margin + fee + slip + buf
        if required > self.balance:
            logger.warning(
                f"[{self.name}] Insufficient balance: need ${required:.4f}, "
                f"have ${self.balance:.4f}")
            return None
        fill         = (price * (1 + CFG["SLIPPAGE"]) if side == "buy"
                        else price * (1 - CFG["SLIPPAGE"]))
        self.balance -= required
        pid           = str(uuid.uuid4())[:12]
        self.positions[pid] = dict(
            symbol=symbol, side=side, entry_price=fill,
            notional=notional, margin=margin, leverage=leverage,
        )
        return pid

    def close_position(self, pid: str,
                       exit_price: float) -> Tuple[Optional[float], Optional[float]]:
        pos = self.positions.pop(pid, None)
        if not pos:
            return None, None
        qty       = pos["notional"] / pos["entry_price"]
        fill_exit = (exit_price * (1 - CFG["SLIPPAGE"]) if pos["side"] == "buy"
                     else exit_price * (1 + CFG["SLIPPAGE"]))
        pnl       = ((fill_exit - pos["entry_price"]) * qty if pos["side"] == "buy"
                     else (pos["entry_price"] - fill_exit) * qty)
        exit_fee  = pos["notional"] * CFG["TAKER_FEE"]
        self.balance += pos["margin"] + pnl - exit_fee
        return pnl, exit_fee

    def apply_funding(self, pid: str, rate: float) -> float:
        pos = self.positions.get(pid)
        if not pos:
            return 0.0
        pnl = (-pos["notional"] * rate if pos["side"] == "buy"
               else  pos["notional"] * rate)
        self.balance += pnl
        return pnl

    async def close(self):
        if self._exchange and not self._exchange.closed:
            await self._exchange.close()
            self._exchange = None


# ══════════════════════════════════════════════════════════════════════════════
#  NETWORK HELP  (module-level so Scanner can call it too)
# ══════════════════════════════════════════════════════════════════════════════
def _print_network_help():
    print("""
  ┌──────────────────────────────────────────────────────────────┐
  │  NETWORK ERROR  —  Possible causes & fixes:                  │
  │                                                              │
  │  1. IP geo-block (India / US / other restricted regions)     │
  │     Fix: Set a proxy in CFG["PROXY"] or env HTTP_PROXY:      │
  │       CFG["PROXY"] = "http://user:pass@proxy_host:port"      │
  │                                                              │
  │  2. Binance 403 (common without VPN/proxy)                   │
  │     Fix: Use a VPN or residential proxy.                     │
  │                                                              │
  │  3. Bybit blocked                                            │
  │     Fix: Same as above.                                      │
  │                                                              │
  │  4. No internet connection                                   │
  │     Fix: Check your network.                                 │
  └──────────────────────────────────────────────────────────────┘
    """)


# ══════════════════════════════════════════════════════════════════════════════
#  SCANNER
# ══════════════════════════════════════════════════════════════════════════════
class Scanner:
    def __init__(self, exchanges: List[CCXTExchange]):
        self.exchanges    = exchanges
        self.symbol_maps: Dict[str, Dict[str, int]] = {}
        self._symbols:    List[str] = []

    async def load_symbols(self):
        print("  Loading symbols via CCXT …")
        for ex in self.exchanges:
            self.symbol_maps[ex.name] = await ex.load_symbols()
            n = len(self.symbol_maps[ex.name])
            print(f"    {ex.name}: {n} symbols")
            if n == 0:
                print(f"    [!] {ex.name} returned 0 symbols — check connectivity/proxy.")

        if not all(self.symbol_maps.values()):
            logger.error("One or more exchanges returned 0 symbols.")
            self._symbols = []
            return

        bad           = {"UP", "DOWN", "BEAR", "BULL", "3L", "3S"}
        common        = set.intersection(*[set(m) for m in self.symbol_maps.values()])
        self._symbols = sorted(
            s for s in common
            if not any(x in s for x in bad)
        )
        print(f"  Common tradeable symbols: {len(self._symbols)}")
        if not self._symbols:
            logger.error("Zero common symbols — both exchanges must load successfully.")

    @property
    def symbols(self) -> List[str]:
        return self._symbols

    # ── Timing helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _hour_end(now: datetime) -> datetime:
        return now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

    def _fires_this_hour(self, nft: Optional[datetime], now: datetime) -> bool:
        if not nft:
            return False
        cutoff = self._hour_end(now) + timedelta(seconds=CFG["HOUR_GRACE_SEC"])
        return now <= nft <= cutoff

    def _freq_bonus(self, interval_min: int) -> float:
        return CFG["FREQ_BONUS"].get(interval_min, CFG["FREQ_BONUS_DEFAULT"])

    def _timing_bonus(self, ttf_min: float) -> float:
        if ttf_min <= 0:
            return 1.0
        return 1.0 + CFG["TIMING_ALPHA"] / ttf_min

    # ── Main scan ─────────────────────────────────────────────────────────────

    async def scan_all(self) -> List[Opportunity]:
        """
        Scans ALL common symbols.
        Total API calls: 2 per exchange (funding_rates + tickers).

        KEY FIX vs v6:
          _build() no longer returns None when "neither fires this hour".
          Every symbol with rate data on both exchanges appears in the output,
          sorted by raw_diff so the top-10 always reflects the real best spreads.
        """
        if not self.symbol_maps:
            await self.load_symbols()

        now      = datetime.now(timezone.utc)
        hour_end = self._hour_end(now)
        print(
            f"  Scanning {len(self._symbols)} symbols  |  "
            f"{now.strftime('%H:%M:%S')} UTC → {hour_end.strftime('%H:%M')} UTC …",
            end=" ", flush=True,
        )

        # Batch fetch concurrently
        fetch_results = await asyncio.gather(
            *[ex.fetch_market_data() for ex in self.exchanges],
            return_exceptions=True,
        )

        ex_market: Dict[str, Tuple[Dict, Dict]] = {}
        for ex, res in zip(self.exchanges, fetch_results):
            if isinstance(res, Exception):
                logger.error(f"[{ex.name}] fetch failed: {res}")
                ex_market[ex.name] = ({}, {})
            else:
                ex_market[ex.name] = res

        ex_a, ex_b = self.exchanges[0], self.exchanges[1]
        fr_a, tk_a = ex_market[ex_a.name]
        fr_b, tk_b = ex_market[ex_b.name]

        n_with_data = 0
        opps: List[Opportunity] = []

        for sym in self._symbols:
            a = ex_a.parse_exchange_data(sym, fr_a.get(sym), tk_a.get(sym))
            b = ex_b.parse_exchange_data(sym, fr_b.get(sym), tk_b.get(sym))
            opp = self._build(a, b, now)
            if opp is not None:
                opps.append(opp)
                n_with_data += 1

        # ── Sort ALL opps by raw_diff descending for display ──────────────────
        opps.sort(key=lambda x: x.raw_diff, reverse=True)

        eligible_count   = sum(1 for o in opps if o.eligible)
        ineligible_count = len(opps) - eligible_count
        print(
            f"done.  {n_with_data}/{len(self._symbols)} have rate data.  "
            f"{eligible_count} eligible  /  {ineligible_count} ineligible."
        )
        return opps

    def _build(self, a: ExchangeData, b: ExchangeData,
               now: datetime) -> Optional[Opportunity]:
        """
        Build an Opportunity for every symbol that has:
          - price on both exchanges
          - funding rate on at least one exchange

        v7 change: NO early return if "neither fires this hour".
        That was the bug that caused most coins to be silently skipped.
        """
        # Need prices on both sides
        if a.price is None or b.price is None:
            return None

        # Need at least one rate to compute a diff
        if a.raw_rate is None and b.raw_rate is None:
            return None

        r_a = a.raw_rate or 0.0
        r_b = b.raw_rate or 0.0

        # ── raw_diff: always computed from actual rates (ignores timing) ──────
        raw_diff = abs(r_a - r_b)

        # ── fires this hour? ──────────────────────────────────────────────────
        a_fires = self._fires_this_hour(a.next_funding, now)
        b_fires = self._fires_this_hour(b.next_funding, now)

        # ── effective rates (timing-aware, used only for trading) ─────────────
        eff_a    = r_a if a_fires else 0.0
        eff_b    = r_b if b_fires else 0.0
        adj_diff = abs(eff_a - eff_b)

        # ── direction (short the higher rate) ─────────────────────────────────
        if r_a >= r_b:
            short_ex, long_ex = a.name, b.name
        else:
            short_ex, long_ex = b.name, a.name

        # ── price gap ─────────────────────────────────────────────────────────
        mid           = (a.price + b.price) / 2
        price_gap_pct = abs(a.price - b.price) / mid if mid > 0 else 0.0

        # ── net pct (trading metric) ──────────────────────────────────────────
        net_pct = adj_diff - ROUND_TRIP_COST - price_gap_pct * 0.5

        # ── timing (only meaningful when at least one fires this hour) ────────
        firing_times: List[datetime] = []
        if a_fires and a.next_funding:
            firing_times.append(a.next_funding)
        if b_fires and b.next_funding:
            firing_times.append(b.next_funding)

        if firing_times:
            target_ft    = min(firing_times)
            later_ft     = max(firing_times)
            ttf_min      = max(0.0, (target_ft - now).total_seconds() / 60)
            entry_open   = target_ft - timedelta(minutes=CFG["ENTRY_WINDOW_MIN"])
            planned_exit = later_ft  + timedelta(seconds=CFG["EXIT_DELAY_SEC"])
        else:
            # Neither fires this hour — use next known funding time for context
            candidates = [t for t in [a.next_funding, b.next_funding] if t]
            target_ft    = min(candidates) if candidates else None
            later_ft     = max(candidates) if candidates else None
            ttf_min      = (max(0.0, (target_ft - now).total_seconds() / 60)
                            if target_ft else 9999.0)
            entry_open   = None
            planned_exit = None

        # ── leverage ──────────────────────────────────────────────────────────
        lev = (CFG["LEV_WIDE"] if price_gap_pct > CFG["PRICE_GAP_LOW_LEV"]
               else CFG["LEV_TIGHT"])
        lev = min(lev, CFG["MAX_LEVERAGE"])

        # ── scoring (for trading, not ranking) ───────────────────────────────
        firing_intervals = ([a.interval_min] if a_fires else []) + \
                           ([b.interval_min] if b_fires else [])
        freq_bonus   = (self._freq_bonus(min(firing_intervals))
                        if firing_intervals else CFG["FREQ_BONUS_DEFAULT"])
        timing_bonus = min(self._timing_bonus(ttf_min) if ttf_min < 9999 else 1.0, 5.0)
        score        = (max(net_pct, 0) * freq_bonus * timing_bonus
                        - price_gap_pct * 0.3)

        avg_vol = ((a.volume_24h or 0) + (b.volume_24h or 0)) / 2

        # ── eligibility checks (trading only) ────────────────────────────────
        eligible = True
        reasons: List[str] = []

        if not a_fires and not b_fires:
            eligible = False
            reasons.append("neither exchange fires this hour")

        if price_gap_pct > CFG["PRICE_GAP_SKIP"]:
            eligible = False
            reasons.append(
                f"price gap {price_gap_pct*100:.3f}% > "
                f"{CFG['PRICE_GAP_SKIP']*100:.1f}%")

        if net_pct <= 0:
            eligible = False
            reasons.append(f"net {net_pct*100:.4f}% <= 0 after fees+slippage")

        if avg_vol < CFG["MIN_VOLUME_USDT"]:
            eligible = False
            reasons.append(f"vol ${avg_vol:,.0f} < ${CFG['MIN_VOLUME_USDT']:,}")

        if ttf_min < CFG["MIN_TIME_TO_FUNDING_MIN"] and ttf_min < 9999:
            eligible = False
            reasons.append(
                f"only {ttf_min:.1f} min to funding "
                f"(min {CFG['MIN_TIME_TO_FUNDING_MIN']})")

        if entry_open and now < entry_open:
            wait_m   = int((entry_open - now).total_seconds()) // 60
            wait_s   = int((entry_open - now).total_seconds()) %  60
            eligible = False
            reasons.append(f"entry window opens in {wait_m}m {wait_s}s")

        return Opportunity(
            symbol               = a.symbol,
            ex_a                 = a,
            ex_b                 = b,
            raw_rate_a           = r_a,
            raw_rate_b           = r_b,
            raw_diff             = raw_diff,
            eff_rate_a           = eff_a,
            eff_rate_b           = eff_b,
            fires_this_hour_a    = a_fires,
            fires_this_hour_b    = b_fires,
            short_exchange       = short_ex,
            long_exchange        = long_ex,
            adj_diff             = adj_diff,
            net_pct              = net_pct,
            price_gap_pct        = price_gap_pct,
            target_funding_time  = target_ft,
            later_funding_time   = later_ft,
            time_to_funding_min  = ttf_min if ttf_min < 9999 else 0.0,
            entry_open_time      = entry_open,
            planned_exit_time    = planned_exit,
            leverage             = lev,
            freq_bonus           = freq_bonus,
            timing_bonus         = timing_bonus,
            score                = score,
            eligible             = eligible,
            skip_reason          = " | ".join(reasons),
        )

    def best(self, opps: List[Opportunity]) -> Optional[Opportunity]:
        eligible = [o for o in opps if o.eligible]
        # Among eligible, sort by score (not raw_diff) for trading decision
        eligible.sort(key=lambda x: x.score, reverse=True)
        return eligible[0] if eligible else None


# ══════════════════════════════════════════════════════════════════════════════
#  DISPLAY
# ══════════════════════════════════════════════════════════════════════════════
def _sep(c="-", w=W): print(c * w)
def _hdr(title):      _sep("="); print(f"  {title}"); _sep("=")


def show_top10(opps: List[Opportunity], best_sym: Optional[str]):
    """
    Prints top-N coins sorted by raw_diff (highest absolute funding rate
    difference between Binance and Bybit), regardless of whether funding
    fires this hour.
    """
    now = datetime.now(timezone.utc)
    n   = CFG["TOP_N"]
    _hdr(
        f"TOP {n} FUNDING RATE SPREAD  |  "
        f"{now.strftime('%Y-%m-%d  %H:%M:%S UTC')}  |  "
        f"Sorted by raw rate diff (ALL coins)"
    )

    # Header
    print(
        f"  {'#':>3}  {'Symbol':<12}  "
        f"{'BIN rate':>9}  {'BYB rate':>9}  "
        f"{'RawDiff':>8}  "
        f"{'Short':>6}  {'Long':>6}  "
        f"{'BIN fires':>9}  {'BYB fires':>9}  "
        f"{'NetPct':>8}  {'Gap%':>6}  "
        f"{'Vol(M)':>7}  {'Lev':>4}  "
        f"{'FundIn':>7}  Decision"
    )
    _sep("-")

    displayed = opps[:n]
    for i, o in enumerate(displayed, 1):
        bin_raw   = o.raw_rate_a if o.ex_a.name == "binance" else o.raw_rate_b
        byb_raw   = o.raw_rate_a if o.ex_a.name == "bybit"   else o.raw_rate_b
        bin_fires = o.fires_this_hour_a if o.ex_a.name == "binance" else o.fires_this_hour_b
        byb_fires = o.fires_this_hour_a if o.ex_a.name == "bybit"   else o.fires_this_hour_b

        avg_vol  = ((o.ex_a.volume_24h or 0) + (o.ex_b.volume_24h or 0)) / 2
        s_tag    = o.short_exchange[:3].upper()
        l_tag    = o.long_exchange[:3].upper()

        bin_f_str = "YES [->]" if bin_fires else "no"
        byb_f_str = "YES [->]" if byb_fires else "no"

        if o.eligible:
            decision = ">> TRADE" if to_display(o.symbol) == best_sym else "   QUEUE"
        else:
            decision = "   skip "

        fund_in = f"{o.time_to_funding_min:>6.1f}m" if o.time_to_funding_min > 0 else "  n/a  "

        print(
            f"  {i:>3}  {to_display(o.symbol):<12}  "
            f"{bin_raw*100:>+9.4f}  {byb_raw*100:>+9.4f}  "
            f"{o.raw_diff*100:>8.4f}  "
            f"{s_tag:>6}  {l_tag:>6}  "
            f"{bin_f_str:>9}  {byb_f_str:>9}  "
            f"{o.net_pct*100:>+8.4f}  {o.price_gap_pct*100:>6.3f}  "
            f"${avg_vol/1e6:>5.1f}M  {o.leverage:.1f}x  "
            f"{fund_in}  {decision}"
        )
        if not o.eligible:
            # Show skip reason on next line, indented
            print(f"        skip reason: {o.skip_reason}")

    _sep("-")
    total_scanned = len(opps)
    eligible_cnt  = sum(1 for o in opps if o.eligible)
    print(
        f"  Scanned: {total_scanned} coins  |  Eligible for trading: {eligible_cnt}  |  "
        f"Round-trip cost: {ROUND_TRIP_COST*100:.3f}%  |  "
        f"fires_this_hour: nextFundingTime <= next x:00 UTC + {CFG['HOUR_GRACE_SEC']}s  |  "
        f"Sorted by: raw rate diff"
    )
    _sep()


def show_best(opp: Opportunity):
    _hdr("BEST OPPORTUNITY SELECTED FOR TRADING")
    bin_raw   = opp.raw_rate_a if opp.ex_a.name == "binance" else opp.raw_rate_b
    byb_raw   = opp.raw_rate_a if opp.ex_a.name == "bybit"   else opp.raw_rate_b
    bin_fires = opp.fires_this_hour_a if opp.ex_a.name == "binance" else opp.fires_this_hour_b
    byb_fires = opp.fires_this_hour_a if opp.ex_a.name == "bybit"   else opp.fires_this_hour_b

    d = to_display(opp.symbol)
    print(f"  Coin                : {d}")
    print(f"  Direction           : SHORT {opp.short_exchange.upper()}"
          f"  x  LONG {opp.long_exchange.upper()}")
    print(f"  " + "-" * 60)
    print(f"  Binance raw rate    : {bin_raw*100:>+.4f}%"
          f"  {'<-- fires this hour' if bin_fires else '<-- NOT firing this hour'}")
    print(f"  Bybit   raw rate    : {byb_raw*100:>+.4f}%"
          f"  {'<-- fires this hour' if byb_fires else '<-- NOT firing this hour'}")
    print(f"  Raw diff            : {opp.raw_diff*100:.4f}%  (|BIN - BYB|)")
    print(f"  Adj diff (eff)      : {opp.adj_diff*100:.4f}%  (timing-aware)")
    print(f"  Round-trip cost     : {ROUND_TRIP_COST*100:.4f}%")
    print(f"  Net expected pct    : {opp.net_pct*100:>+.4f}%")
    print(f"  Price gap           : {opp.price_gap_pct*100:.3f}%")
    print(f"  Leverage            : {opp.leverage:.1f}x")
    print(f"  " + "-" * 60)
    print(f"  Freq bonus          : {opp.freq_bonus:.2f}x")
    print(f"  Timing bonus        : {opp.timing_bonus:.2f}x")
    print(f"  Score               : {opp.score*1000:>+.4f}  (x1000)")
    print(f"  " + "-" * 60)
    print(f"  Time to funding     : {opp.time_to_funding_min:.1f} min")
    if opp.target_funding_time:
        print(f"  Target funding      : {opp.target_funding_time.strftime('%H:%M:%S UTC')}")
    if opp.later_funding_time:
        print(f"  Later  funding      : {opp.later_funding_time.strftime('%H:%M:%S UTC')}")
    if opp.planned_exit_time:
        print(f"  Planned exit        : {opp.planned_exit_time.strftime('%H:%M:%S UTC')}")
    print(f"  " + "=" * 60)
    print(f"  TRADE DECISION      : >> ENTER")
    _sep()


def show_trade_opened(t: TradeRecord, bin_bal: float, byb_bal: float):
    _sep()
    print("  [>>] PAPER TRADE OPENED")
    _sep()
    print(f"  Trade ID    : {t.trade_id}")
    print(f"  Symbol      : {to_display(t.symbol)}")
    print(f"  LONG  {t.long_exchange.upper():<8}  notional ${t.notional:,.4f}  lev {t.leverage:.1f}x")
    print(f"  SHORT {t.short_exchange.upper():<8}  notional ${t.notional:,.4f}  lev {t.leverage:.1f}x")
    print(f"  Adj diff    : {t.adj_diff*100:.4f}%   net est: {t.net_pct*100:+.4f}%")
    print(f"  Funding at  : {t.target_funding_time.strftime('%H:%M:%S UTC')}"
          f"  (later: {t.later_funding_time.strftime('%H:%M:%S UTC')})")
    print(f"  Planned exit: {t.planned_exit_time.strftime('%H:%M:%S UTC')}")
    print(f"  BIN balance : ${bin_bal:,.4f}    BYB balance: ${byb_bal:,.4f}")
    _sep()


def show_trade_closed(t: TradeRecord, bin_bal: float, byb_bal: float):
    hold     = ((t.exit_time - t.entry_time).total_seconds() / 60
                if t.exit_time else 0)
    pos_pnl  = (t.pnl_long or 0) + (t.pnl_short or 0)
    fund_pnl = t.funding_pnl_long + t.funding_pnl_short
    fees     = t.entry_fees + (t.exit_fees or 0)
    _sep()
    print("  [CLOSED] PAPER TRADE CLOSED")
    _sep()
    print(f"  Trade ID    : {t.trade_id}   Symbol: {to_display(t.symbol)}   held {hold:.1f} min")
    print(f"  Position PnL: ${pos_pnl:>+.6f}")
    print(f"  Funding PnL : ${fund_pnl:>+.6f}  "
          f"{'[Y] collected' if t.funding_collected else '[N] not collected'}")
    print(f"  Total fees  : ${-fees:>+.6f}")
    print(f"  * NET TOTAL : ${t.total_pnl:>+.6f}")
    print(f"  BIN balance : ${bin_bal:,.4f}    BYB balance: ${byb_bal:,.4f}")
    _sep()


def show_history(history: List[TradeRecord]):
    if not history:
        return
    _hdr("TRADE HISTORY")
    print(
        f"  {'ID':<14} {'Symbol':<12} {'Entry':>16} {'Hold':>6} "
        f"{'Notional':>10} {'Lev':>4} {'PosPnL':>10} "
        f"{'FundPnL':>10} {'Fees':>9} {'Net':>11}"
    )
    _sep("-")
    total = 0.0
    for t in history:
        hold     = ((t.exit_time - t.entry_time).total_seconds() / 60
                    if t.exit_time else 0)
        pos_pnl  = (t.pnl_long or 0) + (t.pnl_short or 0)
        fund_net = t.funding_pnl_long + t.funding_pnl_short
        fees     = t.entry_fees + (t.exit_fees or 0)
        net      = t.total_pnl or 0
        total   += net
        e        = t.entry_time.strftime("%m-%d %H:%M")
        print(
            f"  {t.trade_id:<14} {to_display(t.symbol):<12} {e:>16} {hold:>6.1f} "
            f"${t.notional*2:>9,.2f} {t.leverage:.1f}x "
            f"{pos_pnl:>+10,.4f} {fund_net:>+10,.4f} "
            f"{-fees:>+9,.4f} {net:>+11,.4f}"
        )
    _sep("-")
    print(f"  {'TOTAL':>83}:  ${total:>+12,.4f}")
    _sep()


def show_account(bin_ex: CCXTExchange, byb_ex: CCXTExchange,
                 active: Optional[TradeRecord], history: List[TradeRecord]):
    _hdr("ACCOUNT SUMMARY")
    margin = sum(p["margin"] for p in
                 {**bin_ex.positions, **byb_ex.positions}.values())
    free   = bin_ex.balance + byb_ex.balance
    total  = free + margin
    start  = CFG["INITIAL_BALANCE"] * 2
    ret    = (total - start) / start * 100 if start else 0
    cum    = sum(t.total_pnl or 0 for t in history)
    print(f"  Binance free cash : ${bin_ex.balance:>18,.4f}")
    print(f"  Bybit   free cash : ${byb_ex.balance:>18,.4f}")
    print(f"  Margin in use     : ${margin:>18,.4f}")
    print(f"  Total acct value  : ${total:>18,.4f}")
    _sep("-")
    print(f"  Start balance     : ${start:>18,.4f}")
    print(f"  Overall PnL       : ${total - start:>+18,.4f}")
    print(f"  Return            : {ret:>+17.4f}%")
    print(f"  Active trade      : {active.trade_id if active else 'None'}")
    print(f"  Closed trades     : {len(history)}"
          f"  ->  cumulative ${cum:>+.4f}")
    _sep()


# ══════════════════════════════════════════════════════════════════════════════
#  BOT
# ══════════════════════════════════════════════════════════════════════════════
class FundingBot:
    def __init__(self):
        self.binance      = CCXTExchange("binance")
        self.bybit        = CCXTExchange("bybit")
        self.scanner      = Scanner([self.binance, self.bybit])
        self.active_trade: Optional[TradeRecord] = None
        self.history:      List[TradeRecord]     = []
        self._counter     = 0
        self.is_running   = False

    # ── CSV persistence ───────────────────────────────────────────────────────

    _FIELDS = [
        "trade_id", "symbol", "long_exchange", "short_exchange",
        "entry_time", "exit_time",
        "entry_price_long", "entry_price_short",
        "exit_price_long",  "exit_price_short",
        "notional", "leverage",
        "eff_rate_long", "eff_rate_short", "adj_diff", "net_pct",
        "target_funding_time", "later_funding_time", "planned_exit_time",
        "pos_id_long", "pos_id_short",
        "entry_fees", "pnl_long", "pnl_short",
        "funding_pnl_long", "funding_pnl_short", "funding_collected",
        "exit_fees", "total_pnl", "status",
    ]

    def _save_trade(self, t: TradeRecord):
        exists = os.path.isfile(CFG["CSV_FILE"])
        with open(CFG["CSV_FILE"], "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=self._FIELDS)
            if not exists:
                w.writeheader()
            row = {k: getattr(t, k, "") for k in self._FIELDS}
            for k in ["entry_time", "exit_time", "target_funding_time",
                      "later_funding_time", "planned_exit_time"]:
                if row[k]:
                    row[k] = row[k].isoformat()
            w.writerow(row)

    def _load_history(self):
        path = CFG["CSV_FILE"]
        if not os.path.isfile(path):
            print("  No trade history file found — fresh start.")
            return
        print(f"  Loading history from {path} …")
        n = 0
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("status") != "CLOSED":
                    continue
                try:
                    t = TradeRecord(
                        trade_id            = row["trade_id"],
                        symbol              = row["symbol"],
                        long_exchange       = row["long_exchange"],
                        short_exchange      = row["short_exchange"],
                        entry_time          = datetime.fromisoformat(row["entry_time"]),
                        exit_time           = (datetime.fromisoformat(row["exit_time"])
                                               if row.get("exit_time") else None),
                        entry_price_long    = float(row["entry_price_long"]),
                        entry_price_short   = float(row["entry_price_short"]),
                        exit_price_long     = (float(row["exit_price_long"])
                                               if row.get("exit_price_long") else None),
                        exit_price_short    = (float(row["exit_price_short"])
                                               if row.get("exit_price_short") else None),
                        notional            = float(row["notional"]),
                        leverage            = float(row["leverage"]),
                        eff_rate_long       = float(row["eff_rate_long"]),
                        eff_rate_short      = float(row["eff_rate_short"]),
                        adj_diff            = float(row["adj_diff"]),
                        net_pct             = float(row["net_pct"]),
                        target_funding_time = datetime.fromisoformat(row["target_funding_time"]),
                        later_funding_time  = datetime.fromisoformat(row["later_funding_time"]),
                        planned_exit_time   = datetime.fromisoformat(row["planned_exit_time"]),
                        pos_id_long         = row["pos_id_long"],
                        pos_id_short        = row["pos_id_short"],
                        entry_fees          = float(row.get("entry_fees",  0)),
                        pnl_long            = (float(row["pnl_long"])
                                               if row.get("pnl_long") else None),
                        pnl_short           = (float(row["pnl_short"])
                                               if row.get("pnl_short") else None),
                        funding_pnl_long    = float(row.get("funding_pnl_long",  0)),
                        funding_pnl_short   = float(row.get("funding_pnl_short", 0)),
                        funding_collected   = (row.get("funding_collected", "False") == "True"),
                        exit_fees           = float(row.get("exit_fees", 0)),
                        total_pnl           = (float(row["total_pnl"])
                                               if row.get("total_pnl") else None),
                        status              = "CLOSED",
                    )
                    self.history.append(t)
                    pnl_sum = t.total_pnl or 0
                    self.binance.balance += pnl_sum / 2
                    self.bybit.balance   += pnl_sum / 2
                    n += 1
                    try:
                        num = int(t.trade_id.split("-")[0])
                        self._counter = max(self._counter, num + 1)
                    except Exception:
                        pass
                except Exception as e:
                    logger.warning(f"CSV row parse error: {e}")
        print(f"  Loaded {n} closed trades.")

    # ── Trade lifecycle ───────────────────────────────────────────────────────

    async def open_trade(self, opp: Opportunity) -> bool:
        if opp.planned_exit_time is None or opp.target_funding_time is None:
            print("  Cannot open trade — funding times not available.")
            return False

        long_ex  = self.binance if opp.long_exchange  == "binance" else self.bybit
        short_ex = self.binance if opp.short_exchange == "binance" else self.bybit
        lev      = opp.leverage

        long_cap  = long_ex.balance  * CFG["CAPITAL_PCT"]
        short_cap = short_ex.balance * CFG["CAPITAL_PCT"]
        usable    = min(long_cap, short_cap)
        if usable < 0.5:
            print("  Insufficient capital to open trade.")
            return False
        notional = usable * lev

        price_long  = (opp.ex_a.price if opp.ex_a.name == opp.long_exchange
                       else opp.ex_b.price)
        price_short = (opp.ex_a.price if opp.ex_a.name == opp.short_exchange
                       else opp.ex_b.price)

        long_id = long_ex.open_position(opp.symbol, "buy", price_long, notional, lev)
        if not long_id:
            print(f"  Could not open LONG on {long_ex.name}")
            return False

        short_id = short_ex.open_position(opp.symbol, "sell", price_short, notional, lev)
        if not short_id:
            long_ex.close_position(long_id, price_long)
            print(f"  Could not open SHORT on {short_ex.name}")
            return False

        entry_fees     = 2 * notional * CFG["TAKER_FEE"]
        self._counter += 1
        eff_rate_long  = (opp.eff_rate_a if opp.ex_a.name == opp.long_exchange
                          else opp.eff_rate_b)
        eff_rate_short = (opp.eff_rate_a if opp.ex_a.name == opp.short_exchange
                          else opp.eff_rate_b)

        self.active_trade = TradeRecord(
            trade_id            = f"{self._counter}-{to_display(opp.symbol)[:8]}",
            symbol              = opp.symbol,
            long_exchange       = opp.long_exchange,
            short_exchange      = opp.short_exchange,
            entry_time          = datetime.now(timezone.utc),
            exit_time           = None,
            entry_price_long    = price_long,
            entry_price_short   = price_short,
            exit_price_long     = None,
            exit_price_short    = None,
            notional            = notional,
            leverage            = lev,
            eff_rate_long       = eff_rate_long,
            eff_rate_short      = eff_rate_short,
            adj_diff            = opp.adj_diff,
            net_pct             = opp.net_pct,
            target_funding_time = opp.target_funding_time,
            later_funding_time  = opp.later_funding_time,
            planned_exit_time   = opp.planned_exit_time,
            pos_id_long         = long_id,
            pos_id_short        = short_id,
            entry_fees          = entry_fees,
        )
        show_trade_opened(self.active_trade, self.binance.balance, self.bybit.balance)
        return True

    def _apply_funding_if_due(self):
        t = self.active_trade
        if t is None or t.funding_collected:
            return
        if datetime.now(timezone.utc) >= t.later_funding_time:
            long_ex  = self.binance if t.long_exchange  == "binance" else self.bybit
            short_ex = self.binance if t.short_exchange == "binance" else self.bybit
            pnl_l = long_ex.apply_funding(t.pos_id_long,   t.eff_rate_long)
            pnl_s = short_ex.apply_funding(t.pos_id_short, t.eff_rate_short)
            t.funding_pnl_long  = pnl_l
            t.funding_pnl_short = pnl_s
            t.funding_collected = True
            logger.info(
                f"[{t.trade_id}] Funding collected: "
                f"long ${pnl_l:+.4f}  short ${pnl_s:+.4f}")

    async def close_trade(self, reason: str = "planned exit"):
        t = self.active_trade
        if t is None:
            return
        logger.info(f"Closing {t.trade_id} [{reason}]")

        long_ex  = self.binance if t.long_exchange  == "binance" else self.bybit
        short_ex = self.binance if t.short_exchange == "binance" else self.bybit

        xl = await long_ex.get_exit_price(t.symbol, t.entry_price_long)
        xs = await short_ex.get_exit_price(t.symbol, t.entry_price_short)

        pnl_l, fee_l = long_ex.close_position(t.pos_id_long,   xl)
        pnl_s, fee_s = short_ex.close_position(t.pos_id_short, xs)
        if pnl_l is None or pnl_s is None:
            print(f"  close_position failed for {t.trade_id}")
            return

        exit_fees = (fee_l or 0) + (fee_s or 0)
        total_pnl = (pnl_l + pnl_s
                     + t.funding_pnl_long + t.funding_pnl_short
                     - t.entry_fees - exit_fees)

        t.exit_time        = datetime.now(timezone.utc)
        t.exit_price_long  = xl
        t.exit_price_short = xs
        t.pnl_long         = pnl_l
        t.pnl_short        = pnl_s
        t.exit_fees        = exit_fees
        t.total_pnl        = total_pnl
        t.status           = "CLOSED"

        self.history.append(t)
        self._save_trade(t)
        show_trade_closed(t, self.binance.balance, self.bybit.balance)
        self.active_trade = None

    # ── Startup ───────────────────────────────────────────────────────────────

    async def initialize(self):
        _hdr(
            f"FUNDING RATE ARBITRAGE BOT v7  (Full-Scan Edition)  |  "
            f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
        )
        proxy = CFG.get("PROXY") or os.environ.get("HTTP_PROXY") or "None"
        print(f"  Data source       : CCXT  (ccxt v{ccxt_async.__version__})")
        print(f"  Exchanges         : binanceusdm  +  bybit (linear)")
        print(f"  API keys needed   : No  (public data only)")
        print(f"  Proxy             : {proxy}")
        print(f"  Initial balance   : ${CFG['INITIAL_BALANCE']:,.2f} per exchange")
        print(f"  Round-trip cost   : {ROUND_TRIP_COST*100:.4f}%")
        print(f"  Scan interval     : {CFG['SCAN_INTERVAL_IDLE']}s  (idle + active)")
        print(f"  Top-N display     : {CFG['TOP_N']} coins sorted by raw rate diff")
        print(f"  v7 key fix        : ALL coins scanned (no early-exit on timing gate)")
        print(f"  Strategy          : ONE best trade, auto-exit after funding")
        _sep()
        self._load_history()
        if self.history:
            show_history(self.history)
        await self.scanner.load_symbols()

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def run(self):
        print("\n  >> Running — press Ctrl+C to stop.\n")
        self.is_running = True
        cycle = 0
        try:
            while self.is_running:
                cycle += 1
                now = datetime.now(timezone.utc)
                _sep("-")
                print(
                    f"  Cycle {cycle}  |  {now.strftime('%H:%M:%S UTC')}  |  "
                    f"BIN ${self.binance.balance:,.4f}  "
                    f"BYB ${self.bybit.balance:,.4f}  |  "
                    f"Active: "
                    f"{'YES - ' + to_display(self.active_trade.symbol) if self.active_trade else 'none'}"
                )
                _sep("-")

                # 1. Apply funding if due
                self._apply_funding_if_due()

                # 2. Close if past planned exit
                if (self.active_trade
                        and now >= self.active_trade.planned_exit_time):
                    await self.close_trade("planned exit — funding collected")

                # 3. Scan all coins
                opps = await self.scanner.scan_all()
                best = (self.scanner.best(opps)
                        if self.active_trade is None else None)

                # 4. Show top-N sorted by raw rate diff
                show_top10(opps, to_display(best.symbol) if best else None)

                # 5. Trade decision
                if self.active_trade is None:
                    if best:
                        show_best(best)
                        await self.open_trade(best)
                    else:
                        print("  No eligible opportunity this cycle — waiting…")
                else:
                    t    = self.active_trade
                    left = (t.planned_exit_time - now).total_seconds()
                    fund_left = (t.later_funding_time - now).total_seconds() / 60
                    print(
                        f"\n  [=] {to_display(t.symbol)}  "
                        f"L:{t.long_exchange.upper()} S:{t.short_exchange.upper()}  "
                        f"exit in {left/60:.1f} min  "
                        f"funding: "
                        f"{'[Y] collected' if t.funding_collected else f'in {fund_left:.1f} min'}"
                    )

                show_account(self.binance, self.bybit, self.active_trade, self.history)

                # 6. Wait 60 seconds before next scan
                interval = (CFG["SCAN_INTERVAL_ACTIVE"] if self.active_trade
                            else CFG["SCAN_INTERVAL_IDLE"])
                print(f"  Next scan in {interval}s …")
                await asyncio.sleep(interval)

        except KeyboardInterrupt:
            print("\n  Stopped by user.")
        finally:
            if self.active_trade:
                await self.close_trade("bot stopped")
            show_history(self.history)
            show_account(self.binance, self.bybit, None, self.history)
            await self.binance.close()
            await self.bybit.close()


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
async def main():
    bot = FundingBot()
    await bot.initialize()
    await bot.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n  Bye!")