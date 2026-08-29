#!/usr/bin/env python3
"""
================================================================================
 FUNDING RATE + PRICE DIFFERENCE ARBITRAGE BOT — PAPER TRADING WITH DASHBOARD
================================================================================

This bot scans all USDT‑perpetual swaps on Binance and Bybit for two strategies:
  1) Funding rate arbitrage – open a position when a profitable funding spread exists.
  2) Price difference arbitrage – open a market‑neutral position when the price gap
     between the same contract on the two exchanges is wider than costs.

All trading is simulated (paper). Market data is fetched live via CCXT.

Requirements:
    pip install ccxt flask

Run:
    python arbitrage_bot.py
================================================================================
"""

import asyncio
import csv
import os
import signal
import sys
import threading
import traceback
import uuid
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Tuple

try:
    import ccxt.async_support as ccxt
except ImportError:
    print("This bot requires the 'ccxt' package.\n"
          "Install it with:  pip install ccxt")
    sys.exit(1)

try:
    from flask import Flask, jsonify, render_template_string
except ImportError:
    print("Web dashboard requires 'flask'.\nInstall with:  pip install flask")
    sys.exit(1)


# ==============================================================================
# CONFIGURATION
# ==============================================================================

CONFIG = {
    # ---------- Funding arbitrage settings ----------
    "HOUR_GRACE_SEC": 120,
    "ENTRY_WINDOW_MIN": 8,
    "MIN_TIME_TO_FUNDING_MIN": 5.0,
    "EXIT_DELAY_SEC": 180,
    "TAKER_FEE": 0.0004,
    "SLIPPAGE": 0.0002,
    "BUFFER_RATE": 0.0010,
    "PRICE_GAP_SKIP": 0.01,
    "PRICE_GAP_LOW_LEV": 0.003,
    "LEV_TIGHT": 4.0,
    "LEV_WIDE": 2.5,
    "MAX_LEVERAGE": 4.0,
    "MIN_VOLUME_USDT": 1_000_000,
    "INITIAL_BALANCE": 27.48,
    "CAPITAL_PCT": 0.90,
    "SCAN_INTERVAL": 60,
    "CSV_FILE": "trades.csv",
    "PROXY": None,
    "TIMING_ALPHA": 2.0,
    "TOP_N_DISPLAY": 10,
    "EXCLUDE_KEYWORDS": ("UP", "DOWN", "BEAR", "BULL", "3L", "3S"),
    "FREQ_BONUS": {60: 1.5, 120: 1.3, 240: 1.2, 480: 1.0},
    "DEFAULT_FUNDING_INTERVAL_MIN": 480,
    "MAX_RETRIES": 3,
    "RETRY_DELAY_SEC": 3,
    "FETCH_TIMEOUT_SEC": 30,        # max seconds to wait for market data fetch

    # ---------- Price difference arbitrage settings ----------
    "PRICE_ARB_ENABLED": True,
    "PRICE_ARB_MIN_GAP": 0.002,           # 0.2% minimum gap to enter
    "PRICE_ARB_EXIT_GAP": 0.0005,         # exit when gap below 0.05%
    "PRICE_ARB_MAX_HOLD_MIN": 60,         # max hold time (minutes)
    "PRICE_ARB_LEVERAGE": 1.0,            # 1x = no leverage
    "PRICE_ARB_CAPITAL_PCT": 0.5,         # use 50% of free balance per side
    "PRICE_ARB_MIN_VOLUME_USDT": 1_000_000,
    "PRICE_ARB_CSV_FILE": "price_trades.csv",
    "PRICE_ARB_ROUND_TRIP_COST": 2 * 0.0004 + 2 * 0.0002,  # fees + slippage (both sides)
    "PRICE_ARB_BUFFER": 0.0002,           # additional safety margin

    # ---------- Web dashboard settings ----------
    "ENABLE_WEB": True,
    "WEB_PORT": int(os.environ.get("PORT", 5000)),
    "WEB_HOST": "0.0.0.0",
}
CONFIG["ROUND_TRIP_COST"] = 4 * CONFIG["TAKER_FEE"] + 2 * CONFIG["SLIPPAGE"]

CSV_FIELDS = [
    "trade_id", "symbol", "long_exchange", "short_exchange",
    "entry_time", "entry_price_long", "entry_price_short",
    "notional", "leverage", "eff_rate_long", "eff_rate_short",
    "adj_diff", "net_pct",
    "target_funding_time", "later_funding_time", "planned_exit_time",
    "pos_id_long", "pos_id_short", "entry_fees", "status",
    "funding_collected", "funding_pnl_long", "funding_pnl_short",
    "exit_time", "exit_price_long", "exit_price_short",
    "pnl_long", "pnl_short", "exit_fees", "total_pnl",
]

PRICE_CSV_FIELDS = [
    "trade_id", "symbol", "buy_exchange", "sell_exchange",
    "entry_time", "entry_price_buy", "entry_price_sell",
    "notional", "leverage", "price_gap",
    "pos_id_buy", "pos_id_sell", "entry_fees", "status",
    "exit_time", "exit_price_buy", "exit_price_sell",
    "pnl_buy", "pnl_sell", "exit_fees", "total_pnl",
]


# ==============================================================================
# HELPERS & DATA CLASSES
# ==============================================================================

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def next_hour_boundary(now: datetime) -> datetime:
    return now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

def to_float(v, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except (TypeError, ValueError):
        return default

def parse_dt(v: Optional[str]) -> Optional[datetime]:
    if not v:
        return None
    try:
        dt = datetime.fromisoformat(v)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None

def fmt_time(dt: Optional[datetime]) -> str:
    return dt.strftime("%H:%M:%S") if dt else "-"


@dataclass
class ExchangeData:
    name: str
    symbol: str
    raw_rate: Optional[float]
    next_funding: Optional[datetime]
    interval_min: float
    price: Optional[float]
    volume_24h: Optional[float]


@dataclass
class Opportunity:
    symbol: str
    ex_a: ExchangeData
    ex_b: ExchangeData
    raw_rate_a: float
    raw_rate_b: float
    raw_diff: float
    eff_rate_a: float
    eff_rate_b: float
    a_fires: bool
    b_fires: bool
    short_exchange: str
    long_exchange: str
    adj_diff: float
    net_pct: float
    price_gap_pct: float
    target_ft: Optional[datetime]
    later_ft: Optional[datetime]
    ttf_min: float
    entry_open: Optional[datetime]
    planned_exit: Optional[datetime]
    leverage: float
    freq_bonus: float
    timing_bonus: float
    score: float
    eligible: bool
    skip_reason: str


@dataclass
class PriceOpportunity:
    symbol: str
    buy_exchange: str
    sell_exchange: str
    buy_price: float
    sell_price: float
    price_gap_pct: float
    volume: float
    eligible: bool
    skip_reason: str


@dataclass
class TradeRecord:
    trade_id: str
    symbol: str
    long_exchange: str
    short_exchange: str
    entry_time: Optional[datetime]
    entry_price_long: float
    entry_price_short: float
    notional: float
    leverage: float
    eff_rate_long: float
    eff_rate_short: float
    adj_diff: float
    net_pct: float
    target_funding_time: Optional[datetime]
    later_funding_time: Optional[datetime]
    planned_exit_time: Optional[datetime]
    pos_id_long: str
    pos_id_short: str
    entry_fees: float
    status: str = "OPEN"
    funding_collected: bool = False
    funding_pnl_long: float = 0.0
    funding_pnl_short: float = 0.0
    exit_time: Optional[datetime] = None
    exit_price_long: Optional[float] = None
    exit_price_short: Optional[float] = None
    pnl_long: Optional[float] = None
    pnl_short: Optional[float] = None
    exit_fees: Optional[float] = None
    total_pnl: Optional[float] = None


@dataclass
class PriceTradeRecord:
    trade_id: str
    symbol: str
    buy_exchange: str
    sell_exchange: str
    entry_time: Optional[datetime]
    entry_price_buy: float
    entry_price_sell: float
    notional: float
    leverage: float
    price_gap: float
    pos_id_buy: str
    pos_id_sell: str
    entry_fees: float
    status: str = "OPEN"
    exit_time: Optional[datetime] = None
    exit_price_buy: Optional[float] = None
    exit_price_sell: Optional[float] = None
    pnl_buy: Optional[float] = None
    pnl_sell: Optional[float] = None
    exit_fees: Optional[float] = None
    total_pnl: Optional[float] = None


# ==============================================================================
# CCXT EXCHANGE WRAPPER
# ==============================================================================

class CCXTExchange:
    def __init__(self, name: str):
        self.name = name
        klass = getattr(ccxt, name)
        params = {
            "enableRateLimit": False,   # Rate limit off: we do 1 bulk fetch per 60s cycle
            "options": {"defaultType": "swap"},
            "timeout": CONFIG["FETCH_TIMEOUT_SEC"] * 1000,  # CCXT uses milliseconds
        }
        if CONFIG["PROXY"]:
            params["proxies"] = {"http": CONFIG["PROXY"], "https": CONFIG["PROXY"]}
        self.exchange = klass(params)
        self.balance: float = CONFIG["INITIAL_BALANCE"]
        self.positions: Dict[str, dict] = {}
        self.symbols: Dict[str, float] = {}

    async def load_symbols(self) -> Dict[str, float]:
        markets = None
        for attempt in range(1, CONFIG["MAX_RETRIES"] + 1):
            try:
                markets = await self.exchange.load_markets()
                break
            except Exception as e:
                print(f"[{self.name}] load_markets failed "
                      f"(attempt {attempt}/{CONFIG['MAX_RETRIES']}): {e}")
                if attempt < CONFIG["MAX_RETRIES"]:
                    await asyncio.sleep(CONFIG["RETRY_DELAY_SEC"])
        if not markets:
            self.symbols = {}
            return {}
        result: Dict[str, float] = {}
        for symbol, market in markets.items():
            try:
                if not market.get("active", True):
                    continue
                if market.get("quote") != "USDT":
                    continue
                if market.get("type") != "swap":
                    continue
                if not market.get("linear", False):
                    continue
                base = (market.get("base") or "").upper()
                if any(kw in base for kw in CONFIG["EXCLUDE_KEYWORDS"]):
                    continue
                result[symbol] = self._extract_funding_interval(market)
            except Exception:
                continue
        self.symbols = result
        return result

    def _extract_funding_interval(self, market: dict) -> float:
        info = market.get("info") or {}
        try:
            if self.name == "binance":
                hours = info.get("fundingIntervalHours")
                if hours:
                    return float(hours) * 60.0
            elif self.name == "bybit":
                minutes = info.get("fundingInterval")
                if minutes:
                    return float(minutes)
        except (TypeError, ValueError):
            pass
        return float(CONFIG["DEFAULT_FUNDING_INTERVAL_MIN"])

    async def fetch_market_data(self) -> Tuple[dict, dict]:
        try:
            funding_rates, tickers = await asyncio.wait_for(
                self._raw_fetch(),
                timeout=CONFIG["FETCH_TIMEOUT_SEC"]
            )
        except asyncio.TimeoutError:
            print(f"[{self.name}] fetch_market_data timed out after {CONFIG['FETCH_TIMEOUT_SEC']}s")
            return {}, {}
        return funding_rates, tickers

    async def _raw_fetch(self) -> Tuple[dict, dict]:
        funding_rates, tickers = await asyncio.gather(
            self.exchange.fetch_funding_rates(),
            self.exchange.fetch_tickers(),
        )
        return funding_rates, tickers

    def parse_exchange_data(self, symbol: str, fr: Optional[dict],
                             tk: Optional[dict]) -> ExchangeData:
        raw_rate = None
        next_funding = None
        price = None
        volume = None
        interval_min = self.symbols.get(symbol, CONFIG["DEFAULT_FUNDING_INTERVAL_MIN"])
        if fr:
            raw_rate = fr.get("fundingRate")
            ts = fr.get("fundingTimestamp")
            if ts:
                try:
                    next_funding = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
                except (TypeError, ValueError, OSError):
                    next_funding = None
        if tk:
            price = tk.get("last") or tk.get("close")
            volume = tk.get("quoteVolume")
            if volume is None:
                base_vol = tk.get("baseVolume")
                if base_vol is not None and price:
                    volume = base_vol * price
        return ExchangeData(
            name=self.name, symbol=symbol, raw_rate=raw_rate,
            next_funding=next_funding, interval_min=interval_min,
            price=price, volume_24h=volume,
        )

    def open_position(self, symbol: str, side: str, price: Optional[float],
                       notional: float, leverage: float) -> Optional[str]:
        if not price or price <= 0 or notional <= 0 or leverage <= 0:
            return None
        margin = notional / leverage
        fee = notional * CONFIG["TAKER_FEE"]
        slip_cost = notional * CONFIG["SLIPPAGE"]
        buffer_cost = notional * CONFIG["BUFFER_RATE"]
        required = margin + fee + slip_cost + buffer_cost
        if required > self.balance:
            return None
        fill_price = price * (1 + CONFIG["SLIPPAGE"]) if side == "buy" \
            else price * (1 - CONFIG["SLIPPAGE"])
        self.balance -= required
        pid = str(uuid.uuid4())[:8]
        self.positions[pid] = {
            "symbol": symbol, "side": side, "entry_price": fill_price,
            "notional": notional, "margin": margin, "leverage": leverage,
        }
        return pid

    def close_position(self, pid: str, exit_price: Optional[float]
                        ) -> Tuple[Optional[float], Optional[float]]:
        pos = self.positions.pop(pid, None)
        if not pos or not exit_price:
            return None, None
        side = pos["side"]
        fill_exit = exit_price * (1 - CONFIG["SLIPPAGE"]) if side == "buy" \
            else exit_price * (1 + CONFIG["SLIPPAGE"])
        qty = pos["notional"] / pos["entry_price"]
        if side == "buy":
            pnl = (fill_exit - pos["entry_price"]) * qty
        else:
            pnl = (pos["entry_price"] - fill_exit) * qty
        exit_fee = pos["notional"] * CONFIG["TAKER_FEE"]
        self.balance += pos["margin"] + pnl - exit_fee
        return pnl, exit_fee

    def apply_funding(self, pid: str, rate: Optional[float]) -> float:
        pos = self.positions.get(pid)
        if not pos or rate is None:
            return 0.0
        pnl = -pos["notional"] * rate if pos["side"] == "buy" else pos["notional"] * rate
        self.balance += pnl
        return pnl

    async def get_exit_price(self, symbol: str, fallback: float) -> float:
        try:
            ticker = await self.exchange.fetch_ticker(symbol)
            price = ticker.get("last") or ticker.get("close")
            return float(price) if price else fallback
        except Exception:
            return fallback

    async def close(self):
        try:
            await self.exchange.close()
        except Exception:
            pass


# ==============================================================================
# SCANNER
# ==============================================================================

class Scanner:
    def __init__(self, exchanges: List[CCXTExchange]):
        self.exchanges = exchanges
        self.common_symbols: List[str] = []

    async def load_symbols(self) -> List[str]:
        symbol_sets = []
        for ex in self.exchanges:
            m = await ex.load_symbols()
            symbol_sets.append(set(m.keys()))
        common = set.intersection(*symbol_sets) if symbol_sets else set()
        self.common_symbols = sorted(common)
        return self.common_symbols

    async def scan_all(self) -> List[Opportunity]:
        now = utcnow()
        ex_a, ex_b = self.exchanges[0], self.exchanges[1]
        try:
            (fr_a, tk_a), (fr_b, tk_b) = await asyncio.gather(
                ex_a.fetch_market_data(),
                ex_b.fetch_market_data(),
            )
        except Exception as e:
            print(f"[scanner] fetch_market_data failed: {e}")
            return [], []
        if not tk_a or not tk_b:
            print("[scanner] Empty ticker data — skipping cycle")
            return [], []

        # --- Funding arb opportunities ---
        opportunities: List[Opportunity] = []
        for symbol in self.common_symbols:
            fr_sym_a, tk_sym_a = fr_a.get(symbol), tk_a.get(symbol)
            fr_sym_b, tk_sym_b = fr_b.get(symbol), tk_b.get(symbol)
            if not (tk_sym_a and tk_sym_b):
                continue
            data_a = ex_a.parse_exchange_data(symbol, fr_sym_a, tk_sym_a)
            data_b = ex_b.parse_exchange_data(symbol, fr_sym_b, tk_sym_b)
            opp = self._build_opportunity(data_a, data_b, now)
            if opp:
                opportunities.append(opp)
        opportunities.sort(key=lambda o: o.raw_diff, reverse=True)

        # --- Price arb opportunities (reuse same ticker data) ---
        price_opps: List[PriceOpportunity] = self._build_price_opportunities(tk_a, tk_b)

        return opportunities, price_opps

    def _build_price_opportunities(self, tk_a: dict, tk_b: dict) -> List[PriceOpportunity]:
        ex_a, ex_b = self.exchanges[0], self.exchanges[1]
        result = []
        for symbol in self.common_symbols:
            ticker_a = tk_a.get(symbol)
            ticker_b = tk_b.get(symbol)
            if not ticker_a or not ticker_b:
                continue
            price_a = to_float(ticker_a.get("last") or ticker_a.get("close"))
            price_b = to_float(ticker_b.get("last") or ticker_b.get("close"))
            if not price_a or not price_b:
                continue
            avg = (price_a + price_b) / 2.0
            gap = abs(price_a - price_b) / avg if avg else 0.0
            if gap < CONFIG["PRICE_ARB_MIN_GAP"] * 0.5:  # skip tiny gaps early
                continue

            vol_a = to_float(ticker_a.get("quoteVolume"))
            if not vol_a and price_a:
                vol_a = to_float(ticker_a.get("baseVolume")) * price_a
            vol_b = to_float(ticker_b.get("quoteVolume"))
            if not vol_b and price_b:
                vol_b = to_float(ticker_b.get("baseVolume")) * price_b
            min_vol = min(vol_a, vol_b)

            if price_a < price_b:
                buy_ex, sell_ex = ex_a.name, ex_b.name
                buy_price, sell_price = price_a, price_b
            else:
                buy_ex, sell_ex = ex_b.name, ex_a.name
                buy_price, sell_price = price_b, price_a

            eligible = True
            reasons = []
            if gap < CONFIG["PRICE_ARB_MIN_GAP"]:
                eligible = False
                reasons.append(f"gap<{CONFIG['PRICE_ARB_MIN_GAP']*100:.2f}%")
            if min_vol < CONFIG["PRICE_ARB_MIN_VOLUME_USDT"]:
                eligible = False
                reasons.append("low volume")

            result.append(PriceOpportunity(
                symbol=symbol,
                buy_exchange=buy_ex,
                sell_exchange=sell_ex,
                buy_price=buy_price,
                sell_price=sell_price,
                price_gap_pct=gap,
                volume=min_vol,
                eligible=eligible,
                skip_reason="; ".join(reasons),
            ))
        result.sort(key=lambda x: x.price_gap_pct, reverse=True)
        return result

    # Keep for compatibility but now unused in main cycle
    async def scan_price_opportunities(self) -> List[PriceOpportunity]:
        ex_a, ex_b = self.exchanges[0], self.exchanges[1]
        try:
            _, tk_a = await ex_a.fetch_market_data()
            _, tk_b = await ex_b.fetch_market_data()
            if not tk_a or not tk_b:
                return []
            return self._build_price_opportunities(tk_a, tk_b)
        except Exception as e:
            print(f"[price scanner] fetch failed: {e}")
            return []

    def _build_opportunity(self, a: ExchangeData, b: ExchangeData,
                            now: datetime) -> Optional[Opportunity]:
        if a.price is None or b.price is None:
            return None
        if a.raw_rate is None and b.raw_rate is None:
            return None
        r_a = a.raw_rate or 0.0
        r_b = b.raw_rate or 0.0
        hour_end = next_hour_boundary(now)
        grace = timedelta(seconds=CONFIG["HOUR_GRACE_SEC"])
        a_fires = bool(a.next_funding and a.next_funding <= hour_end + grace)
        b_fires = bool(b.next_funding and b.next_funding <= hour_end + grace)
        eff_a = r_a if a_fires else 0.0
        eff_b = r_b if b_fires else 0.0
        raw_diff = abs(r_a - r_b)
        adj_diff = abs(eff_a - eff_b)
        if eff_a >= eff_b:
            short_ex, long_ex = a.name, b.name
        else:
            short_ex, long_ex = b.name, a.name
        avg_price = (a.price + b.price) / 2.0
        price_gap_pct = abs(a.price - b.price) / avg_price if avg_price else 0.0
        net_pct = raw_diff - CONFIG["ROUND_TRIP_COST"]
        firing_times = []
        if a_fires and a.next_funding:
            firing_times.append(a.next_funding)
        if b_fires and b.next_funding:
            firing_times.append(b.next_funding)
        if firing_times:
            target_ft = min(firing_times)
            later_ft = max(firing_times)
            ttf_min = (target_ft - now).total_seconds() / 60.0
            entry_open = target_ft - timedelta(minutes=CONFIG["ENTRY_WINDOW_MIN"])
            planned_exit = later_ft + timedelta(seconds=CONFIG["EXIT_DELAY_SEC"])
        else:
            target_ft = later_ft = entry_open = planned_exit = None
            ttf_min = 9999.0
        leverage = CONFIG["LEV_WIDE"] if price_gap_pct > CONFIG["PRICE_GAP_LOW_LEV"] \
            else CONFIG["LEV_TIGHT"]
        leverage = min(leverage, CONFIG["MAX_LEVERAGE"])
        firing_intervals = []
        if a_fires:
            firing_intervals.append(a.interval_min)
        if b_fires:
            firing_intervals.append(b.interval_min)
        if firing_intervals:
            freq_bonus = CONFIG["FREQ_BONUS"].get(int(min(firing_intervals)), 1.0)
        else:
            freq_bonus = 1.0
        if 0 < ttf_min < 9999:
            timing_bonus = 1.0 + CONFIG["TIMING_ALPHA"] / ttf_min
        else:
            timing_bonus = 1.0
        score = max(net_pct, 0.0) * freq_bonus * timing_bonus
        vols = [v for v in (a.volume_24h, b.volume_24h) if v is not None]
        avg_vol = sum(vols) / len(vols) if vols else 0.0
        eligible = True
        reasons = []
        if target_ft is None:
            eligible = False
            reasons.append("no upcoming funding")
        elif net_pct <= 0:
            eligible = False
            reasons.append("net<=0")
        if avg_vol < CONFIG["MIN_VOLUME_USDT"]:
            eligible = False
            reasons.append("low volume")
        if price_gap_pct > CONFIG["PRICE_GAP_SKIP"]:
            eligible = False
            reasons.append(f"price gap {price_gap_pct*100:.2f}% > {CONFIG['PRICE_GAP_SKIP']*100:.1f}%")
        if 0 < ttf_min < CONFIG["MIN_TIME_TO_FUNDING_MIN"]:
            eligible = False
            reasons.append("too close to funding")
        if entry_open and now < entry_open:
            eligible = False
            reasons.append("entry window not open yet")
        return Opportunity(
            symbol=a.symbol, ex_a=a, ex_b=b,
            raw_rate_a=r_a, raw_rate_b=r_b, raw_diff=raw_diff,
            eff_rate_a=eff_a, eff_rate_b=eff_b,
            a_fires=a_fires, b_fires=b_fires,
            short_exchange=short_ex, long_exchange=long_ex,
            adj_diff=adj_diff, net_pct=net_pct, price_gap_pct=price_gap_pct,
            target_ft=target_ft, later_ft=later_ft, ttf_min=ttf_min,
            entry_open=entry_open, planned_exit=planned_exit,
            leverage=leverage, freq_bonus=freq_bonus, timing_bonus=timing_bonus,
            score=score, eligible=eligible, skip_reason="; ".join(reasons),
        )

    @staticmethod
    def best(opportunities: List[Opportunity]) -> Optional[Opportunity]:
        eligible = [o for o in opportunities if o.eligible]
        if not eligible:
            return None
        eligible.sort(key=lambda o: o.score, reverse=True)
        return eligible[0]


# ==============================================================================
# WEB DASHBOARD
# ==============================================================================

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Arbitrage Bot Dashboard</title>
    <meta charset="UTF-8">
    <meta http-equiv="refresh" content="15">
    <style>
        body { font-family: Arial, sans-serif; background: #f4f6f9; margin: 20px; }
        h1 { color: #2c3e50; }
        .container { max-width: 1400px; margin: auto; }
        .card { background: white; border-radius: 8px; padding: 15px; margin-bottom: 20px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
        .summary { display: flex; flex-wrap: wrap; gap: 15px; }
        .summary-item { flex: 1; min-width: 150px; background: #ecf0f1; padding: 10px; border-radius: 5px; }
        .summary-item .label { font-size: 0.9em; color: #7f8c8d; }
        .summary-item .value { font-size: 1.2em; font-weight: bold; }
        .table-wrap { overflow-x: auto; }
        table { width: 100%; border-collapse: collapse; font-size: 0.9em; }
        th { background: #34495e; color: white; padding: 8px; text-align: left; }
        td { padding: 6px 8px; border-bottom: 1px solid #ddd; }
        tr:hover { background: #f1f9ff; }
        .positive { color: green; }
        .negative { color: red; }
        .badge { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 0.8em; }
        .badge-eligible { background: #2ecc71; color: white; }
        .badge-skip { background: #e74c3c; color: white; }
        .badge-open { background: #f39c12; color: white; }
        .badge-closed { background: #95a5a6; color: white; }
        .mono { font-family: monospace; }
        .timestamp { color: #7f8c8d; font-size: 0.8em; }
        .footer { text-align: center; margin-top: 20px; color: #7f8c8d; }
    </style>
</head>
<body>
<div class="container">
    <h1>🚀 Arbitrage Bot — Funding & Price Diff</h1>
    <div id="status" class="card">
        <div class="summary" id="summary"></div>
    </div>

    <div class="card">
        <h2>📊 Top Funding Opportunities</h2>
        <div class="table-wrap"><table id="funding-opps-table"><thead><tr>
            <th>#</th><th>Symbol</th><th>Binance Rate%</th><th>Bybit Rate%</th><th>Raw Diff%</th>
            <th>Short</th><th>Long</th><th>Net%</th><th>Gap%</th><th>Decision</th>
        </tr></thead><tbody id="funding-opps-body"></tbody></table></div>
    </div>

    <div class="card">
        <h2>💰 Top Price Diff Opportunities</h2>
        <div class="table-wrap"><table id="price-opps-table"><thead><tr>
            <th>#</th><th>Symbol</th><th>Buy Exch</th><th>Sell Exch</th><th>Buy Price</th>
            <th>Sell Price</th><th>Gap%</th><th>Volume (M)</th><th>Decision</th>
        </tr></thead><tbody id="price-opps-body"></tbody></table></div>
    </div>

    <div class="card">
        <h2>📈 Active Trades</h2>
        <div id="active-trades">
            <div><strong>Funding Trade:</strong> <span id="active-funding-trade">None</span></div>
            <div><strong>Price Trade:</strong> <span id="active-price-trade">None</span></div>
        </div>
    </div>

    <div class="card">
        <h2>📜 Trade History (last 5 each)</h2>
        <h3>Funding Trades</h3>
        <div class="table-wrap"><table id="funding-history-table"><thead><tr>
            <th>ID</th><th>Symbol</th><th>Hold</th><th>Notional</th><th>Net PnL</th>
        </tr></thead><tbody id="funding-history-body"></tbody></table></div>
        <h3>Price Trades</h3>
        <div class="table-wrap"><table id="price-history-table"><thead><tr>
            <th>ID</th><th>Symbol</th><th>Hold</th><th>Notional</th><th>Net PnL</th>
        </tr></thead><tbody id="price-history-body"></tbody></table></div>
    </div>

    <div class="footer">Auto‑refresh every 15 seconds</div>
</div>
<script>
function fetchData() {
    fetch('/api/status')
        .then(res => res.json())
        .then(data => {
            // Summary
            const summary = document.getElementById('summary');
            const s = data.summary;
            summary.innerHTML = `
                <div class="summary-item"><div class="label">Binance Cash</div><div class="value">$${s.binance_balance.toFixed(4)}</div></div>
                <div class="summary-item"><div class="label">Bybit Cash</div><div class="value">$${s.bybit_balance.toFixed(4)}</div></div>
                <div class="summary-item"><div class="label">Margin in Use</div><div class="value">$${s.margin_use.toFixed(4)}</div></div>
                <div class="summary-item"><div class="label">Total Equity</div><div class="value">$${s.equity.toFixed(4)}</div></div>
                <div class="summary-item"><div class="label">P&L</div><div class="value ${s.pnl >= 0 ? 'positive' : 'negative'}">${s.pnl >= 0 ? '+' : ''}${s.pnl.toFixed(4)} (${s.return_pct.toFixed(2)}%)</div></div>
                <div class="summary-item"><div class="label">Funding Active</div><div class="value">${s.active_funding_trade || 'None'}</div></div>
                <div class="summary-item"><div class="label">Price Active</div><div class="value">${s.active_price_trade || 'None'}</div></div>
                <div class="summary-item"><div class="label">Closed Trades</div><div class="value">${s.closed_funding_trades} / ${s.closed_price_trades}</div></div>
                <div class="summary-item"><div class="label">Scans</div><div class="value">${s.scans}</div></div>
            `;

            // Funding opportunities
            const fundingBody = document.getElementById('funding-opps-body');
            fundingBody.innerHTML = data.funding_opportunities.map((o, i) => `
                <tr>
                    <td>${i+1}</td>
                    <td><strong>${o.symbol}</strong></td>
                    <td>${(o.raw_rate_a * 100).toFixed(4)}</td>
                    <td>${(o.raw_rate_b * 100).toFixed(4)}</td>
                    <td>${(o.raw_diff * 100).toFixed(4)}</td>
                    <td>${o.short_exchange}</td>
                    <td>${o.long_exchange}</td>
                    <td>${(o.net_pct * 100).toFixed(4)}</td>
                    <td>${(o.price_gap_pct * 100).toFixed(3)}</td>
                    <td>${o.eligible ? '<span class="badge badge-eligible">ELIGIBLE</span>' : '<span class="badge badge-skip">SKIP</span>'}</td>
                </tr>
            `).join('');

            // Price opportunities
            const priceBody = document.getElementById('price-opps-body');
            priceBody.innerHTML = data.price_opportunities.map((o, i) => `
                <tr>
                    <td>${i+1}</td>
                    <td><strong>${o.symbol}</strong></td>
                    <td>${o.buy_exchange}</td>
                    <td>${o.sell_exchange}</td>
                    <td>${o.buy_price.toFixed(6)}</td>
                    <td>${o.sell_price.toFixed(6)}</td>
                    <td>${(o.price_gap_pct * 100).toFixed(4)}</td>
                    <td>${(o.volume / 1e6).toFixed(2)}</td>
                    <td>${o.eligible ? '<span class="badge badge-eligible">ELIGIBLE</span>' : '<span class="badge badge-skip">SKIP</span>'}</td>
                </tr>
            `).join('');

            // Active trades
            const activeFundingSpan = document.getElementById('active-funding-trade');
            if (data.active_funding_trade) {
                const t = data.active_funding_trade;
                activeFundingSpan.innerHTML = `${t.trade_id} (${t.symbol}) Long: ${t.long_exchange} / Short: ${t.short_exchange}`;
            } else {
                activeFundingSpan.textContent = 'None';
            }
            const activePriceSpan = document.getElementById('active-price-trade');
            if (data.active_price_trade) {
                const t = data.active_price_trade;
                activePriceSpan.innerHTML = `${t.trade_id} (${t.symbol}) Buy: ${t.buy_exchange} / Sell: ${t.sell_exchange}`;
            } else {
                activePriceSpan.textContent = 'None';
            }

            // Funding history
            const fundHistBody = document.getElementById('funding-history-body');
            fundHistBody.innerHTML = data.funding_history.map(h => {
                const hold = h.hold_min ? h.hold_min.toFixed(1) + 'm' : '-';
                const net = h.total_pnl || 0;
                const netClass = net >= 0 ? 'positive' : 'negative';
                return `<tr>
                    <td>${h.trade_id}</td>
                    <td>${h.symbol}</td>
                    <td>${hold}</td>
                    <td>$${h.notional.toFixed(2)}</td>
                    <td class="${netClass}">${net >= 0 ? '+' : ''}${net.toFixed(4)}</td>
                </tr>`;
            }).join('');

            // Price history
            const priceHistBody = document.getElementById('price-history-body');
            priceHistBody.innerHTML = data.price_history.map(h => {
                const hold = h.hold_min ? h.hold_min.toFixed(1) + 'm' : '-';
                const net = h.total_pnl || 0;
                const netClass = net >= 0 ? 'positive' : 'negative';
                return `<tr>
                    <td>${h.trade_id}</td>
                    <td>${h.symbol}</td>
                    <td>${hold}</td>
                    <td>$${h.notional.toFixed(2)}</td>
                    <td class="${netClass}">${net >= 0 ? '+' : ''}${net.toFixed(4)}</td>
                </tr>`;
            }).join('');
        })
        .catch(err => console.error('Fetch error:', err));
}

// Initial load and auto-refresh
fetchData();
setInterval(fetchData, 15000);
</script>
</body>
</html>
"""

class WebDashboard:
    def __init__(self, bot_state):
        self.bot_state = bot_state
        self.app = Flask(__name__)
        self.app.add_url_rule('/', 'index', self.index)
        self.app.add_url_rule('/api/status', 'status', self.status)
        self.thread = None
        self.stopped = False

    def start(self):
        if CONFIG["ENABLE_WEB"]:
            self.thread = threading.Thread(
                target=self.app.run,
                kwargs={
                    'host': CONFIG["WEB_HOST"],
                    'port': CONFIG["WEB_PORT"],
                    'debug': False,
                    'use_reloader': False,
                    'threaded': True,
                },
                daemon=True
            )
            self.thread.start()
            print(f"Web dashboard started at http://{CONFIG['WEB_HOST']}:{CONFIG['WEB_PORT']}/")

    def stop(self):
        self.stopped = True

    def index(self):
        return render_template_string(HTML_TEMPLATE)

    def status(self):
        state = self.bot_state
        bin_bal = state.get('binance_balance', 0)
        byb_bal = state.get('bybit_balance', 0)
        margin_use = state.get('margin_use', 0)
        equity = bin_bal + byb_bal + margin_use
        initial = CONFIG["INITIAL_BALANCE"] * 2
        pnl = equity - initial
        ret_pct = (pnl / initial * 100) if initial else 0.0

        summary = {
            'binance_balance': bin_bal,
            'bybit_balance': byb_bal,
            'margin_use': margin_use,
            'equity': equity,
            'pnl': pnl,
            'return_pct': ret_pct,
            'active_funding_trade': state.get('active_funding_trade_id'),
            'active_price_trade': state.get('active_price_trade_id'),
            'closed_funding_trades': state.get('closed_funding_trades_count', 0),
            'closed_price_trades': state.get('closed_price_trades_count', 0),
            'scans': state.get('scans', 0),
        }

        # Funding opportunities (top 10)
        funding_opps = state.get('funding_opportunities', [])[:10]
        funding_opp_list = []
        for o in funding_opps:
            funding_opp_list.append({
                'symbol': o.symbol,
                'raw_rate_a': o.raw_rate_a,
                'raw_rate_b': o.raw_rate_b,
                'raw_diff': o.raw_diff,
                'short_exchange': o.short_exchange,
                'long_exchange': o.long_exchange,
                'net_pct': o.net_pct,
                'price_gap_pct': o.price_gap_pct,
                'eligible': o.eligible,
            })

        # Price opportunities (top 10)
        price_opps = state.get('price_opportunities', [])[:10]
        price_opp_list = []
        for o in price_opps:
            price_opp_list.append({
                'symbol': o.symbol,
                'buy_exchange': o.buy_exchange,
                'sell_exchange': o.sell_exchange,
                'buy_price': o.buy_price,
                'sell_price': o.sell_price,
                'price_gap_pct': o.price_gap_pct,
                'volume': o.volume,
                'eligible': o.eligible,
            })

        # Active funding trade
        active_funding = state.get('active_funding_trade')
        active_funding_dict = None
        if active_funding:
            active_funding_dict = {
                'trade_id': active_funding.trade_id,
                'symbol': active_funding.symbol,
                'long_exchange': active_funding.long_exchange,
                'short_exchange': active_funding.short_exchange,
            }

        # Active price trade
        active_price = state.get('active_price_trade')
        active_price_dict = None
        if active_price:
            active_price_dict = {
                'trade_id': active_price.trade_id,
                'symbol': active_price.symbol,
                'buy_exchange': active_price.buy_exchange,
                'sell_exchange': active_price.sell_exchange,
            }

        # Funding history (last 5)
        funding_hist = state.get('funding_history', [])[-5:]
        funding_hist_list = []
        for h in funding_hist:
            hold = None
            if h.entry_time and h.exit_time:
                hold = (h.exit_time - h.entry_time).total_seconds() / 60.0
            funding_hist_list.append({
                'trade_id': h.trade_id,
                'symbol': h.symbol,
                'hold_min': hold,
                'notional': h.notional,
                'total_pnl': h.total_pnl,
            })

        # Price history (last 5)
        price_hist = state.get('price_history', [])[-5:]
        price_hist_list = []
        for h in price_hist:
            hold = None
            if h.entry_time and h.exit_time:
                hold = (h.exit_time - h.entry_time).total_seconds() / 60.0
            price_hist_list.append({
                'trade_id': h.trade_id,
                'symbol': h.symbol,
                'hold_min': hold,
                'notional': h.notional,
                'total_pnl': h.total_pnl,
            })

        return jsonify({
            'summary': summary,
            'funding_opportunities': funding_opp_list,
            'price_opportunities': price_opp_list,
            'active_funding_trade': active_funding_dict,
            'active_price_trade': active_price_dict,
            'funding_history': funding_hist_list,
            'price_history': price_hist_list,
        })


# ==============================================================================
# ARBITRAGE BOT (funding + price)
# ==============================================================================

class ArbitrageBot:
    def __init__(self):
        self.binance = CCXTExchange("binance")
        self.bybit = CCXTExchange("bybit")
        self.scanner = Scanner([self.binance, self.bybit])

        # Funding arbitrage state
        self.active_funding_trade: Optional[TradeRecord] = None
        self.funding_history: List[TradeRecord] = []
        self.funding_counter = 0
        self.stats = {
            "scans": 0,
            "funding_opened": 0,
            "funding_closed": 0,
            "price_opened": 0,
            "price_closed": 0,
            "fees_paid": 0.0,
        }

        # Price arbitrage state
        self.active_price_trade: Optional[PriceTradeRecord] = None
        self.price_history: List[PriceTradeRecord] = []
        self.price_counter = 0

        # Dashboard state
        self.dashboard_state = {
            'binance_balance': self.binance.balance,
            'bybit_balance': self.bybit.balance,
            'margin_use': 0,
            'active_funding_trade_id': None,
            'active_price_trade_id': None,
            'closed_funding_trades_count': 0,
            'closed_price_trades_count': 0,
            'scans': 0,
            'funding_opportunities': [],
            'price_opportunities': [],
            'active_funding_trade': None,
            'active_price_trade': None,
            'funding_history': [],
            'price_history': [],
        }

        self.dashboard = WebDashboard(self.dashboard_state)

    def _update_dashboard_state(self, funding_opps: List[Opportunity],
                                 price_opps: List[PriceOpportunity]):
        self.dashboard_state['binance_balance'] = self.binance.balance
        self.dashboard_state['bybit_balance'] = self.bybit.balance
        margin_b = sum(p["margin"] for p in self.binance.positions.values())
        margin_y = sum(p["margin"] for p in self.bybit.positions.values())
        self.dashboard_state['margin_use'] = margin_b + margin_y
        self.dashboard_state['active_funding_trade_id'] = self.active_funding_trade.trade_id if self.active_funding_trade else None
        self.dashboard_state['active_price_trade_id'] = self.active_price_trade.trade_id if self.active_price_trade else None
        self.dashboard_state['closed_funding_trades_count'] = len(self.funding_history)
        self.dashboard_state['closed_price_trades_count'] = len(self.price_history)
        self.dashboard_state['scans'] = self.stats['scans']
        self.dashboard_state['funding_opportunities'] = funding_opps
        self.dashboard_state['price_opportunities'] = price_opps
        self.dashboard_state['active_funding_trade'] = self.active_funding_trade
        self.dashboard_state['active_price_trade'] = self.active_price_trade
        self.dashboard_state['funding_history'] = self.funding_history
        self.dashboard_state['price_history'] = self.price_history

    def _exchange_by_name(self, name: str) -> CCXTExchange:
        return self.binance if name == "binance" else self.bybit

    async def initialize(self):
        print("Loading common USDT-perpetual symbols from Binance and Bybit...")
        symbols = await self.scanner.load_symbols()
        print(f"Loaded {len(symbols)} common symbols.")
        self._load_history()

    def _load_history(self):
        # Load funding history
        path = CONFIG["CSV_FILE"]
        if os.path.exists(path):
            try:
                with open(path, "r", newline="") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if row.get("status") != "CLOSED":
                            continue
                        total_pnl = to_float(row.get("total_pnl"))
                        half = total_pnl / 2.0
                        self.binance.balance += half
                        self.bybit.balance += half
                        trade = TradeRecord(
                            trade_id=row.get("trade_id", ""),
                            symbol=row.get("symbol", ""),
                            long_exchange=row.get("long_exchange", ""),
                            short_exchange=row.get("short_exchange", ""),
                            entry_time=parse_dt(row.get("entry_time")),
                            entry_price_long=to_float(row.get("entry_price_long")),
                            entry_price_short=to_float(row.get("entry_price_short")),
                            notional=to_float(row.get("notional")),
                            leverage=to_float(row.get("leverage")),
                            eff_rate_long=to_float(row.get("eff_rate_long")),
                            eff_rate_short=to_float(row.get("eff_rate_short")),
                            adj_diff=to_float(row.get("adj_diff")),
                            net_pct=to_float(row.get("net_pct")),
                            target_funding_time=parse_dt(row.get("target_funding_time")),
                            later_funding_time=parse_dt(row.get("later_funding_time")),
                            planned_exit_time=parse_dt(row.get("planned_exit_time")),
                            pos_id_long=row.get("pos_id_long", ""),
                            pos_id_short=row.get("pos_id_short", ""),
                            entry_fees=to_float(row.get("entry_fees")),
                            status="CLOSED",
                            funding_collected=(row.get("funding_collected") == "True"),
                            funding_pnl_long=to_float(row.get("funding_pnl_long")),
                            funding_pnl_short=to_float(row.get("funding_pnl_short")),
                            exit_time=parse_dt(row.get("exit_time")),
                            exit_price_long=to_float(row.get("exit_price_long")),
                            exit_price_short=to_float(row.get("exit_price_short")),
                            pnl_long=to_float(row.get("pnl_long")),
                            pnl_short=to_float(row.get("pnl_short")),
                            exit_fees=to_float(row.get("exit_fees")),
                            total_pnl=total_pnl,
                        )
                        self.funding_history.append(trade)
                        try:
                            num = int(str(trade.trade_id).split("-")[0])
                            self.funding_counter = max(self.funding_counter, num + 1)
                        except (ValueError, IndexError):
                            pass
                if self.funding_history:
                    print(f"Restored {len(self.funding_history)} closed funding trades from {path}.")
            except Exception as e:
                print(f"Warning: failed to load funding history: {e}")

        # Load price history
        price_path = CONFIG["PRICE_ARB_CSV_FILE"]
        if os.path.exists(price_path):
            try:
                with open(price_path, "r", newline="") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if row.get("status") != "CLOSED":
                            continue
                        total_pnl = to_float(row.get("total_pnl"))
                        half = total_pnl / 2.0
                        self.binance.balance += half
                        self.bybit.balance += half
                        trade = PriceTradeRecord(
                            trade_id=row.get("trade_id", ""),
                            symbol=row.get("symbol", ""),
                            buy_exchange=row.get("buy_exchange", ""),
                            sell_exchange=row.get("sell_exchange", ""),
                            entry_time=parse_dt(row.get("entry_time")),
                            entry_price_buy=to_float(row.get("entry_price_buy")),
                            entry_price_sell=to_float(row.get("entry_price_sell")),
                            notional=to_float(row.get("notional")),
                            leverage=to_float(row.get("leverage")),
                            price_gap=to_float(row.get("price_gap")),
                            pos_id_buy=row.get("pos_id_buy", ""),
                            pos_id_sell=row.get("pos_id_sell", ""),
                            entry_fees=to_float(row.get("entry_fees")),
                            status="CLOSED",
                            exit_time=parse_dt(row.get("exit_time")),
                            exit_price_buy=to_float(row.get("exit_price_buy")),
                            exit_price_sell=to_float(row.get("exit_price_sell")),
                            pnl_buy=to_float(row.get("pnl_buy")),
                            pnl_sell=to_float(row.get("pnl_sell")),
                            exit_fees=to_float(row.get("exit_fees")),
                            total_pnl=total_pnl,
                        )
                        self.price_history.append(trade)
                        try:
                            num = int(str(trade.trade_id).split("-")[0])
                            self.price_counter = max(self.price_counter, num + 1)
                        except (ValueError, IndexError):
                            pass
                if self.price_history:
                    print(f"Restored {len(self.price_history)} closed price trades from {price_path}.")
            except Exception as e:
                print(f"Warning: failed to load price history: {e}")

    def _save_funding_trade(self, trade: TradeRecord):
        path = CONFIG["CSV_FILE"]
        file_exists = os.path.exists(path)
        row = asdict(trade)
        for key, value in row.items():
            if isinstance(value, datetime):
                row[key] = value.isoformat()
            elif value is None:
                row[key] = ""
        try:
            with open(path, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                if not file_exists:
                    writer.writeheader()
                writer.writerow({k: row.get(k, "") for k in CSV_FIELDS})
        except Exception as e:
            print(f"Warning: failed to write funding trade to CSV: {e}")

    def _save_price_trade(self, trade: PriceTradeRecord):
        path = CONFIG["PRICE_ARB_CSV_FILE"]
        file_exists = os.path.exists(path)
        row = asdict(trade)
        for key, value in row.items():
            if isinstance(value, datetime):
                row[key] = value.isoformat()
            elif value is None:
                row[key] = ""
        try:
            with open(path, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=PRICE_CSV_FIELDS)
                if not file_exists:
                    writer.writeheader()
                writer.writerow({k: row.get(k, "") for k in PRICE_CSV_FIELDS})
        except Exception as e:
            print(f"Warning: failed to write price trade to CSV: {e}")

    async def open_funding_trade(self, opp: Opportunity) -> bool:
        if not opp.eligible:
            return False
        long_ex = self._exchange_by_name(opp.long_exchange)
        short_ex = self._exchange_by_name(opp.short_exchange)
        lev = opp.leverage
        usable = min(long_ex.balance * CONFIG["CAPITAL_PCT"],
                     short_ex.balance * CONFIG["CAPITAL_PCT"])
        if usable < 0.50:
            return False
        notional = usable * lev
        price_long = opp.ex_a.price if opp.ex_a.name == opp.long_exchange else opp.ex_b.price
        price_short = opp.ex_a.price if opp.ex_a.name == opp.short_exchange else opp.ex_b.price
        long_id = long_ex.open_position(opp.symbol, "buy", price_long, notional, lev)
        if not long_id:
            return False
        short_id = short_ex.open_position(opp.symbol, "sell", price_short, notional, lev)
        if not short_id:
            long_ex.close_position(long_id, price_long)
            return False
        self.funding_counter += 1
        entry_fees = 2 * notional * CONFIG["TAKER_FEE"]
        eff_rate_long = opp.eff_rate_a if opp.ex_a.name == opp.long_exchange else opp.eff_rate_b
        eff_rate_short = opp.eff_rate_a if opp.ex_a.name == opp.short_exchange else opp.eff_rate_b
        trade = TradeRecord(
            trade_id=f"{self.funding_counter}-{opp.symbol[:8]}",
            symbol=opp.symbol,
            long_exchange=opp.long_exchange,
            short_exchange=opp.short_exchange,
            entry_time=utcnow(),
            entry_price_long=price_long,
            entry_price_short=price_short,
            notional=notional,
            leverage=lev,
            eff_rate_long=eff_rate_long,
            eff_rate_short=eff_rate_short,
            adj_diff=opp.adj_diff,
            net_pct=opp.net_pct,
            target_funding_time=opp.target_ft,
            later_funding_time=opp.later_ft,
            planned_exit_time=opp.planned_exit,
            pos_id_long=long_id,
            pos_id_short=short_id,
            entry_fees=entry_fees,
            status="OPEN",
        )
        self.active_funding_trade = trade
        self.stats["funding_opened"] += 1
        self.stats["fees_paid"] += entry_fees
        print(f"\n>>> FUNDING TRADE OPENED: {trade.trade_id} | {trade.symbol}")
        return True

    async def open_price_trade(self, opp: PriceOpportunity) -> bool:
        if not opp.eligible:
            print(f"[price arb] Skipped — not eligible: {opp.skip_reason}")
            return False
        buy_ex = self._exchange_by_name(opp.buy_exchange)
        sell_ex = self._exchange_by_name(opp.sell_exchange)
        lev = CONFIG["PRICE_ARB_LEVERAGE"]
        usable = min(buy_ex.balance * CONFIG["PRICE_ARB_CAPITAL_PCT"],
                     sell_ex.balance * CONFIG["PRICE_ARB_CAPITAL_PCT"])
        print(f"[price arb] Attempting entry: {opp.symbol} | gap={opp.price_gap_pct*100:.4f}% "
              f"| usable=${usable:.4f} | buy_bal=${buy_ex.balance:.4f} sell_bal=${sell_ex.balance:.4f}")
        if usable < 0.50:
            print(f"[price arb] Skipped — insufficient usable balance (${usable:.4f} < $0.50)")
            return False
        notional = usable * lev
        buy_id = buy_ex.open_position(opp.symbol, "buy", opp.buy_price, notional, lev)
        if not buy_id:
            print(f"[price arb] Skipped — open_position (buy) rejected on {buy_ex.name} "
                  f"(bal=${buy_ex.balance:.4f}, notional=${notional:.4f})")
            return False
        sell_id = sell_ex.open_position(opp.symbol, "sell", opp.sell_price, notional, lev)
        if not sell_id:
            print(f"[price arb] Skipped — open_position (sell) rejected on {sell_ex.name}, rolling back buy")
            buy_ex.close_position(buy_id, opp.buy_price)
            return False
        self.price_counter += 1
        entry_fees = 2 * notional * CONFIG["TAKER_FEE"]
        trade = PriceTradeRecord(
            trade_id=f"P{self.price_counter}-{opp.symbol[:8]}",
            symbol=opp.symbol,
            buy_exchange=opp.buy_exchange,
            sell_exchange=opp.sell_exchange,
            entry_time=utcnow(),
            entry_price_buy=opp.buy_price,
            entry_price_sell=opp.sell_price,
            notional=notional,
            leverage=lev,
            price_gap=opp.price_gap_pct,
            pos_id_buy=buy_id,
            pos_id_sell=sell_id,
            entry_fees=entry_fees,
            status="OPEN",
        )
        self.active_price_trade = trade
        self.stats["price_opened"] += 1
        self.stats["fees_paid"] += entry_fees
        print(f"\n>>> PRICE TRADE OPENED: {trade.trade_id} | {trade.symbol} | gap={opp.price_gap_pct*100:.4f}%")
        return True

    def apply_funding_if_due(self):
        t = self.active_funding_trade
        if t is None or t.funding_collected:
            return
        now = utcnow()
        long_ex = self._exchange_by_name(t.long_exchange)
        short_ex = self._exchange_by_name(t.short_exchange)

        # Apply long-leg funding when target_funding_time is reached
        if (t.funding_pnl_long == 0.0
                and t.target_funding_time is not None
                and now >= t.target_funding_time):
            pnl_l = long_ex.apply_funding(t.pos_id_long, t.eff_rate_long)
            t.funding_pnl_long = pnl_l
            print(f"\n*** Funding (long leg) collected for {t.trade_id}: {pnl_l:+.4f}")

        # Apply short-leg funding when later_funding_time is reached
        if (t.funding_pnl_short == 0.0
                and t.later_funding_time is not None
                and now >= t.later_funding_time):
            pnl_s = short_ex.apply_funding(t.pos_id_short, t.eff_rate_short)
            t.funding_pnl_short = pnl_s
            print(f"\n*** Funding (short leg) collected for {t.trade_id}: {pnl_s:+.4f}")

        # Mark fully collected when both legs are done
        both_target_done = (t.target_funding_time is None or now >= t.target_funding_time)
        both_later_done = (t.later_funding_time is None or now >= t.later_funding_time)
        if both_target_done and both_later_done:
            t.funding_collected = True
            total = t.funding_pnl_long + t.funding_pnl_short
            print(f"*** All funding collected for {t.trade_id}: total={total:+.4f}")

    async def close_funding_trade(self, reason: str):
        t = self.active_funding_trade
        if t is None:
            return
        long_ex = self._exchange_by_name(t.long_exchange)
        short_ex = self._exchange_by_name(t.short_exchange)
        xl = await long_ex.get_exit_price(t.symbol, t.entry_price_long)
        xs = await short_ex.get_exit_price(t.symbol, t.entry_price_short)
        pnl_l, fee_l = long_ex.close_position(t.pos_id_long, xl)
        pnl_s, fee_s = short_ex.close_position(t.pos_id_short, xs)
        if pnl_l is None or pnl_s is None:
            print(f"Error closing funding trade {t.trade_id}: one or both positions missing.")
            self.active_funding_trade = None
            return
        exit_fees = (fee_l or 0.0) + (fee_s or 0.0)
        total_pnl = (
            pnl_l + pnl_s
            + t.funding_pnl_long + t.funding_pnl_short
            - t.entry_fees - exit_fees
        )
        t.exit_time = utcnow()
        t.exit_price_long = xl
        t.exit_price_short = xs
        t.pnl_long = pnl_l
        t.pnl_short = pnl_s
        t.exit_fees = exit_fees
        t.total_pnl = total_pnl
        t.status = "CLOSED"
        self.funding_history.append(t)
        self._save_funding_trade(t)
        self.stats["funding_closed"] += 1
        self.stats["fees_paid"] += exit_fees
        print(f"\n<<< FUNDING TRADE CLOSED: {t.trade_id} | reason: {reason}")
        print(f"    Position PnL: long={pnl_l:+.4f}  short={pnl_s:+.4f}")
        print(f"    Funding PnL:  long={t.funding_pnl_long:+.4f}  "
              f"short={t.funding_pnl_short:+.4f}")
        print(f"    TOTAL PnL: {total_pnl:+.4f} USDT")
        self.active_funding_trade = None

    async def close_price_trade(self, reason: str):
        t = self.active_price_trade
        if t is None:
            return
        buy_ex = self._exchange_by_name(t.buy_exchange)
        sell_ex = self._exchange_by_name(t.sell_exchange)
        xb = await buy_ex.get_exit_price(t.symbol, t.entry_price_buy)
        xs = await sell_ex.get_exit_price(t.symbol, t.entry_price_sell)
        pnl_b, fee_b = buy_ex.close_position(t.pos_id_buy, xb)
        pnl_s, fee_s = sell_ex.close_position(t.pos_id_sell, xs)
        if pnl_b is None or pnl_s is None:
            print(f"Error closing price trade {t.trade_id}: one or both positions missing.")
            self.active_price_trade = None
            return
        exit_fees = (fee_b or 0.0) + (fee_s or 0.0)
        total_pnl = pnl_b + pnl_s - t.entry_fees - exit_fees
        t.exit_time = utcnow()
        t.exit_price_buy = xb
        t.exit_price_sell = xs
        t.pnl_buy = pnl_b
        t.pnl_sell = pnl_s
        t.exit_fees = exit_fees
        t.total_pnl = total_pnl
        t.status = "CLOSED"
        self.price_history.append(t)
        self._save_price_trade(t)
        self.stats["price_closed"] += 1
        self.stats["fees_paid"] += exit_fees
        print(f"\n<<< PRICE TRADE CLOSED: {t.trade_id} | reason: {reason}")
        print(f"    Buy PnL: {pnl_b:+.4f}  Sell PnL: {pnl_s:+.4f}")
        print(f"    TOTAL PnL: {total_pnl:+.4f} USDT")
        self.active_price_trade = None

    # ------------------------------------------------------------- display
    def show_top_funding(self, opps: List[Opportunity]):
        n = CONFIG["TOP_N_DISPLAY"]
        header = (f"{'#':<3}{'Symbol':<16}{'BinR%':>8}{'BybR%':>8}{'RawD%':>8}"
                  f"{'Short':>8}{'Long':>8}{'Net%':>8}{'Gap%':>7}{'Vol(M)':>8}"
                  f"{'Lev':>5}{'ExitUTC':>9}  Decision")
        print(f"\n{header}")
        print("-" * 135)
        for i, o in enumerate(opps[:n], 1):
            vols = [v for v in (o.ex_a.volume_24h, o.ex_b.volume_24h) if v is not None]
            avg_vol = (sum(vols) / len(vols)) if vols else 0.0
            decision = "ELIGIBLE" if o.eligible else f"skip: {o.skip_reason}"
            print(f"{i:<3}{o.symbol:<16}{o.raw_rate_a * 100:>8.4f}{o.raw_rate_b * 100:>8.4f}"
                  f"{o.raw_diff * 100:>8.4f}{o.short_exchange:>8}{o.long_exchange:>8}"
                  f"{o.net_pct * 100:>8.4f}{o.price_gap_pct * 100:>7.3f}"
                  f"{avg_vol / 1e6:>8.2f}{o.leverage:>5.1f}"
                  f"{fmt_time(o.planned_exit):>9}  {decision}")

    def show_top_price(self, opps: List[PriceOpportunity]):
        n = CONFIG["TOP_N_DISPLAY"]
        header = (f"{'#':<3}{'Symbol':<16}{'BuyEx':>8}{'SellEx':>8}"
                  f"{'BuyPrice':>12}{'SellPrice':>12}{'Gap%':>8}{'Vol(M)':>8}  Decision")
        print(f"\n{header}")
        print("-" * 110)
        for i, o in enumerate(opps[:n], 1):
            decision = "ELIGIBLE" if o.eligible else f"skip: {o.skip_reason}"
            print(f"{i:<3}{o.symbol:<16}{o.buy_exchange:>8}{o.sell_exchange:>8}"
                  f"{o.buy_price:>12.6f}{o.sell_price:>12.6f}{o.price_gap_pct * 100:>8.4f}"
                  f"{o.volume / 1e6:>8.2f}  {decision}")

    def show_best(self, o: Opportunity):
        print(f"\n*** BEST FUNDING OPPORTUNITY: {o.symbol}")
        print(f"    Short {o.short_exchange} / Long {o.long_exchange}")
        print(f"    Raw diff: {o.raw_diff * 100:.4f}%  |  Adjusted diff: {o.adj_diff * 100:.4f}%")
        print(f"    Net%: {o.net_pct * 100:.4f}%  |  Price gap: {o.price_gap_pct * 100:.3f}%")
        print(f"    Leverage: {o.leverage:.2f}x  |  Score: {o.score:.6f}")
        print(f"    Time to funding: {o.ttf_min:.2f} min  |  "
              f"Planned exit: {fmt_time(o.planned_exit)} UTC")

    def show_account_summary(self):
        b_bal, y_bal = self.binance.balance, self.bybit.balance
        margin_b = sum(p["margin"] for p in self.binance.positions.values())
        margin_y = sum(p["margin"] for p in self.bybit.positions.values())
        equity = b_bal + y_bal + margin_b + margin_y
        initial = CONFIG["INITIAL_BALANCE"] * 2
        pnl = equity - initial
        ret_pct = (pnl / initial * 100) if initial else 0.0
        print("\n--- Account Summary ---")
        print(f"    Binance free cash: ${b_bal:.4f}   |   Bybit free cash: ${y_bal:.4f}")
        print(f"    Margin in use: ${margin_b + margin_y:.4f}")
        print(f"    Total equity: ${equity:.4f}   |   P&L: {pnl:+.4f} ({ret_pct:+.2f}%)")
        print(f"    Funding active: {self.active_funding_trade.trade_id if self.active_funding_trade else 'None'}"
              f"   |   Price active: {self.active_price_trade.trade_id if self.active_price_trade else 'None'}")
        print(f"    Closed: {len(self.funding_history)} funding, {len(self.price_history)} price")

    def show_trade_history(self):
        recent_funding = self.funding_history[-3:]
        recent_price = self.price_history[-3:]
        if recent_funding:
            print(f"\n--- Recent Funding Trades (last {len(recent_funding)}) ---")
            for t in recent_funding:
                hold = "-"
                if t.entry_time and t.exit_time:
                    hold = f"{(t.exit_time - t.entry_time).total_seconds() / 60.0:.1f}m"
                net = t.total_pnl if t.total_pnl is not None else 0.0
                print(f"    {t.trade_id:<14}{t.symbol:<14}hold={hold:<8}"
                      f"notional=${t.notional:>8.2f}  net={net:+.4f}")
        if recent_price:
            print(f"\n--- Recent Price Trades (last {len(recent_price)}) ---")
            for t in recent_price:
                hold = "-"
                if t.entry_time and t.exit_time:
                    hold = f"{(t.exit_time - t.entry_time).total_seconds() / 60.0:.1f}m"
                net = t.total_pnl if t.total_pnl is not None else 0.0
                print(f"    {t.trade_id:<14}{t.symbol:<14}hold={hold:<8}"
                      f"notional=${t.notional:>8.2f}  net={net:+.4f}")

    def show_active_status(self):
        t = self.active_funding_trade
        if t:
            now = utcnow()
            print(f"\n--- Active Funding Trade: {t.trade_id} ({t.symbol}) ---")
            print(f"    Long {t.long_exchange} @ {t.entry_price_long:.6f}  |  "
                  f"Short {t.short_exchange} @ {t.entry_price_short:.6f}")
            if t.funding_collected:
                print(f"    Funding collected: long={t.funding_pnl_long:+.4f}  "
                      f"short={t.funding_pnl_short:+.4f}")
            elif t.later_funding_time:
                wait_s = (t.later_funding_time - now).total_seconds()
                print(f"    Funding not yet collected (~{max(wait_s, 0):.0f}s remaining).")
            if t.planned_exit_time:
                exit_in = (t.planned_exit_time - now).total_seconds()
                print(f"    Planned exit in ~{max(exit_in, 0):.0f}s")

        p = self.active_price_trade
        if p:
            now = utcnow()
            print(f"\n--- Active Price Trade: {p.trade_id} ({p.symbol}) ---")
            print(f"    Buy {p.buy_exchange} @ {p.entry_price_buy:.6f}  |  "
                  f"Sell {p.sell_exchange} @ {p.entry_price_sell:.6f}")
            print(f"    Gap at entry: {p.price_gap*100:.4f}%")

    def _print_banner(self):
        print("=" * 90)
        print(" ARBITRAGE BOT  —  FUNDING RATE + PRICE DIFFERENCE  (PAPER TRADING)")
        print("=" * 90)
        print(f" Symbols tracked   : {len(self.scanner.common_symbols)}")
        print(f" Scan interval     : {CONFIG['SCAN_INTERVAL']}s")
        print(f" Demo balance      : ${CONFIG['INITIAL_BALANCE']:.2f} per exchange "
              f"(${CONFIG['INITIAL_BALANCE'] * 2:.2f} total)")
        print(f" Current balance   : Binance ${self.binance.balance:.4f}  |  "
              f"Bybit ${self.bybit.balance:.4f}")
        print(f" Funding max lev   : {CONFIG['MAX_LEVERAGE']}x")
        print(f" Price arb lev     : {CONFIG['PRICE_ARB_LEVERAGE']}x")
        print(f" Min funding vol   : ${CONFIG['MIN_VOLUME_USDT']:,.0f}")
        print(f" Min price gap     : {CONFIG['PRICE_ARB_MIN_GAP']*100:.2f}%")
        print(f" Round-trip cost   : {CONFIG['ROUND_TRIP_COST']*100:.3f}% (funding)")
        print(f" Trading CSV       : {CONFIG['CSV_FILE']} / {CONFIG['PRICE_ARB_CSV_FILE']}")
        if CONFIG["ENABLE_WEB"]:
            print(f" Web dashboard     : http://{CONFIG['WEB_HOST']}:{CONFIG['WEB_PORT']}/")
        print("=" * 90)

    async def run(self):
        await self.initialize()
        self._print_banner()

        if CONFIG["ENABLE_WEB"]:
            self.dashboard.start()

        stop_event = asyncio.Event()

        def _handle_signal(*args):
            print("\n\nShutdown signal received, closing up...")
            stop_event.set()

        loop = asyncio.get_running_loop()
        try:
            loop.add_signal_handler(signal.SIGINT, _handle_signal)
            loop.add_signal_handler(signal.SIGTERM, _handle_signal)
        except (NotImplementedError, AttributeError):
            signal.signal(signal.SIGINT, lambda *a: _handle_signal())
            signal.signal(signal.SIGTERM, lambda *a: _handle_signal())

        try:
            while not stop_event.is_set():
                try:
                    await self._run_cycle()
                except Exception:
                    print("\n[cycle error] An unexpected error occurred this cycle:")
                    traceback.print_exc()

                try:
                    await asyncio.wait_for(stop_event.wait(),
                                            timeout=CONFIG["SCAN_INTERVAL"])
                except asyncio.TimeoutError:
                    pass
        finally:
            await self._shutdown()

    async def _run_cycle(self):
        now = utcnow()
        self.stats["scans"] += 1

        print("\n" + "=" * 90)
        print(f"CYCLE | {now.strftime('%Y-%m-%d %H:%M:%S UTC')} | "
              f"Binance: ${self.binance.balance:.4f} | Bybit: ${self.bybit.balance:.4f} | "
              f"Fund Active: {self.active_funding_trade.trade_id if self.active_funding_trade else 'None'} | "
              f"Price Active: {self.active_price_trade.trade_id if self.active_price_trade else 'None'}")
        print("=" * 90)

        # ---- Handle funding trade lifecycle ----
        self.apply_funding_if_due()
        if (self.active_funding_trade and self.active_funding_trade.planned_exit_time
                and now >= self.active_funding_trade.planned_exit_time):
            await self.close_funding_trade("planned exit — funding collected")

        # ---- Handle price trade lifecycle ----
        if self.active_price_trade:
            # Check exit condition: gap reduced below exit threshold or max hold time reached
            # Fetch current prices to compute gap
            buy_ex = self._exchange_by_name(self.active_price_trade.buy_exchange)
            sell_ex = self._exchange_by_name(self.active_price_trade.sell_exchange)
            try:
                cur_buy = await buy_ex.get_exit_price(self.active_price_trade.symbol,
                                                       self.active_price_trade.entry_price_buy)
                cur_sell = await sell_ex.get_exit_price(self.active_price_trade.symbol,
                                                         self.active_price_trade.entry_price_sell)
                avg = (cur_buy + cur_sell) / 2
                cur_gap = abs(cur_buy - cur_sell) / avg if avg else 0.0
                # Check max hold time
                hold_min = (utcnow() - self.active_price_trade.entry_time).total_seconds() / 60.0
                breakeven_gap = self.active_price_trade.price_gap - (CONFIG.get("PRICE_ARB_ROUND_TRIP_COST", 0.0012))
                if cur_gap < CONFIG["PRICE_ARB_EXIT_GAP"]:
                    await self.close_price_trade("exit gap reached (0.05%)")
                elif hold_min > 15 and cur_gap <= breakeven_gap:
                    await self.close_price_trade("15+ min hold break-even reached")
                elif hold_min >= CONFIG["PRICE_ARB_MAX_HOLD_MIN"]:
                    await self.close_price_trade("max hold time reached")
            except Exception as e:
                print(f"Error checking price exit: {e}")

        # ---- Scan opportunities (single fetch for both arb types) ----
        scan_result = await self.scanner.scan_all()
        if isinstance(scan_result, tuple):
            funding_opps, price_opps = scan_result
        else:
            funding_opps, price_opps = scan_result, []  # fallback safety
        best_funding = self.scanner.best(funding_opps) if self.active_funding_trade is None else None

        # Price opportunities (already computed in scan_all)
        best_price = None
        if self.active_price_trade is None and price_opps:
            # Choose the most eligible (largest gap)
            eligible_price = [o for o in price_opps if o.eligible]
            print(f"\n[price arb] {len(price_opps)} total opps | {len(eligible_price)} eligible")
            if eligible_price:
                best_price = eligible_price[0]
                print(f"[price arb] Best: {best_price.symbol} gap={best_price.price_gap_pct*100:.4f}%")

        # Update dashboard state
        self._update_dashboard_state(funding_opps, price_opps)

        # Console display
        self.show_top_funding(funding_opps)
        if best_funding:
            self.show_best(best_funding)
        if price_opps:
            self.show_top_price(price_opps)

        # Open trades if no active
        if self.active_funding_trade is None and best_funding:
            await self.open_funding_trade(best_funding)
        else:
            if self.active_funding_trade:
                print("\nFunding trade already active; no new funding entry.")

        if self.active_price_trade is None and best_price:
            await self.open_price_trade(best_price)
        elif self.active_price_trade is None and not best_price:
            print("\nNo eligible price arbitrage opportunity this cycle.")
        elif self.active_price_trade:
            print("\nPrice trade already active; no new price entry.")

        # Show active statuses and summary
        self.show_active_status()
        self.show_account_summary()
        self.show_trade_history()

    async def _shutdown(self):
        self.apply_funding_if_due()

        if self.active_funding_trade:
            print("\nBot stopping — closing active funding trade...")
            try:
                await self.close_funding_trade("bot stopped")
            except Exception:
                print("Error while closing funding trade during shutdown:")
                traceback.print_exc()

        if self.active_price_trade:
            print("\nBot stopping — closing active price trade...")
            try:
                await self.close_price_trade("bot stopped")
            except Exception:
                print("Error while closing price trade during shutdown:")
                traceback.print_exc()

        print("\n" + "=" * 90)
        print(" FINAL SUMMARY")
        print("=" * 90)
        print(f" Scans completed        : {self.stats['scans']}")
        print(f" Funding trades opened  : {self.stats['funding_opened']}")
        print(f" Funding trades closed  : {self.stats['funding_closed']}")
        print(f" Price trades opened    : {self.stats['price_opened']}")
        print(f" Price trades closed    : {self.stats['price_closed']}")
        print(f" Total fees paid        : ${self.stats['fees_paid']:.4f}")

        margin_b = sum(p["margin"] for p in self.binance.positions.values())
        margin_y = sum(p["margin"] for p in self.bybit.positions.values())
        equity = self.binance.balance + self.bybit.balance + margin_b + margin_y
        initial = CONFIG["INITIAL_BALANCE"] * 2
        pnl = equity - initial
        ret_pct = (pnl / initial * 100) if initial else 0.0
        print(f" Final equity           : ${equity:.4f}")
        print(f" Final P&L              : {pnl:+.4f} USDT ({ret_pct:+.2f}%)")
        print("=" * 90)

        await self.binance.close()
        await self.bybit.close()


# ==============================================================================
# MAIN ENTRY POINT
# ==============================================================================

async def _main():
    bot = ArbitrageBot()
    await bot.run()

def main():
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()