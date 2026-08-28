#!/usr/bin/env python3
"""
================================================================================
 FUNDING RATE ARBITRAGE BOT  —  PAPER / DEMO TRADING  WITH WEB DASHBOARD
================================================================================

This bot scans all USDT‑perpetual swaps traded on both Binance and Bybit,
compares their funding rates, and opens a simulated (paper) position when
a profitable spread is detected. All market data is fetched live via CCXT
(public endpoints, no API keys required).

A Flask web dashboard is included, accessible at http://<host>:5000/,
showing live opportunities, active trade, account summary, and trade history.

Requirements:
    pip install ccxt flask

Run:
    python funding_bot.py
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
    # --- Web dashboard settings ---
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

# ==============================================================================
# CCXT EXCHANGE WRAPPER
# ==============================================================================

class CCXTExchange:
    def __init__(self, name: str):
        self.name = name
        klass = getattr(ccxt, name)
        params = {"enableRateLimit": True, "options": {"defaultType": "swap"}}
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
        funding_rates = await self.exchange.fetch_funding_rates()
        tickers = await self.exchange.fetch_tickers()
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
            return []
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
        return opportunities

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
        net_pct = adj_diff - CONFIG["ROUND_TRIP_COST"]
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
    <title>Funding Arbitrage Bot Dashboard</title>
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
    <h1>🚀 Funding Arbitrage Bot — Live Dashboard</h1>
    <div id="status" class="card">
        <div class="summary" id="summary"></div>
    </div>

    <div class="card">
        <h2>📊 Top Opportunities</h2>
        <div class="table-wrap"><table id="opps-table"><thead><tr>
            <th>#</th><th>Symbol</th><th>Binance Rate%</th><th>Bybit Rate%</th><th>Raw Diff%</th>
            <th>Short</th><th>Long</th><th>Bin Fires</th><th>Byb Fires</th><th>Net%</th>
            <th>Gap%</th><th>Vol (M)</th><th>Leverage</th><th>Exit UTC</th><th>Decision</th>
        </tr></thead><tbody id="opps-body"></tbody></table></div>
    </div>

    <div class="card">
        <h2>📈 Active Trade</h2>
        <div id="active-trade">None</div>
    </div>

    <div class="card">
        <h2>📜 Trade History (last 5)</h2>
        <div class="table-wrap"><table id="history-table"><thead><tr>
            <th>ID</th><th>Symbol</th><th>Hold</th><th>Notional</th><th>Leverage</th>
            <th>Pos PnL</th><th>Funding PnL</th><th>Fees</th><th>Net PnL</th>
        </tr></thead><tbody id="history-body"></tbody></table></div>
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
                <div class="summary-item"><div class="label">Active Trade</div><div class="value">${s.active_trade || 'None'}</div></div>
                <div class="summary-item"><div class="label">Closed Trades</div><div class="value">${s.closed_trades}</div></div>
                <div class="summary-item"><div class="label">Scans</div><div class="value">${s.scans}</div></div>
            `;

            // Opportunities
            const oppsBody = document.getElementById('opps-body');
            oppsBody.innerHTML = data.opportunities.map((o, i) => `
                <tr>
                    <td>${i+1}</td>
                    <td><strong>${o.symbol}</strong></td>
                    <td>${(o.raw_rate_a * 100).toFixed(4)}</td>
                    <td>${(o.raw_rate_b * 100).toFixed(4)}</td>
                    <td>${(o.raw_diff * 100).toFixed(4)}</td>
                    <td>${o.short_exchange}</td>
                    <td>${o.long_exchange}</td>
                    <td>${o.a_fires ? '✅' : '❌'}</td>
                    <td>${o.b_fires ? '✅' : '❌'}</td>
                    <td>${(o.net_pct * 100).toFixed(4)}</td>
                    <td>${(o.price_gap_pct * 100).toFixed(3)}</td>
                    <td>${(o.volume || 0).toFixed(2)}</td>
                    <td>${o.leverage.toFixed(1)}x</td>
                    <td>${o.planned_exit || '-'}</td>
                    <td>${o.eligible ? '<span class="badge badge-eligible">ELIGIBLE</span>' : `<span class="badge badge-skip">SKIP</span><br><span style="font-size:0.7em;">${o.skip_reason}</span>`}</td>
                </tr>
            `).join('');

            // Active trade
            const activeDiv = document.getElementById('active-trade');
            if (data.active_trade) {
                const t = data.active_trade;
                activeDiv.innerHTML = `
                    <table><tr><th>ID</th><td>${t.trade_id}</td></tr>
                    <tr><th>Symbol</th><td>${t.symbol}</td></tr>
                    <tr><th>Long</th><td>${t.long_exchange} @ ${t.entry_price_long.toFixed(6)}</td></tr>
                    <tr><th>Short</th><td>${t.short_exchange} @ ${t.entry_price_short.toFixed(6)}</td></tr>
                    <tr><th>Funding</th><td>${t.funding_collected ? 'Collected' : 'Pending'}</td></tr>
                    <tr><th>Planned exit</th><td>${t.planned_exit || '-'}</td></tr>
                    </table>
                `;
            } else {
                activeDiv.innerHTML = '<em>No active trade</em>';
            }

            // History
            const histBody = document.getElementById('history-body');
            histBody.innerHTML = data.history.map(h => {
                const hold = h.hold_min ? h.hold_min.toFixed(1) + 'm' : '-';
                const posPnL = (h.pnl_long || 0) + (h.pnl_short || 0);
                const fundPnL = (h.funding_pnl_long || 0) + (h.funding_pnl_short || 0);
                const fees = (h.entry_fees || 0) + (h.exit_fees || 0);
                const net = h.total_pnl || 0;
                const netClass = net >= 0 ? 'positive' : 'negative';
                return `<tr>
                    <td>${h.trade_id}</td>
                    <td>${h.symbol}</td>
                    <td>${hold}</td>
                    <td>$${h.notional.toFixed(2)}</td>
                    <td>${h.leverage.toFixed(1)}x</td>
                    <td>${posPnL.toFixed(4)}</td>
                    <td>${fundPnL.toFixed(4)}</td>
                    <td>${(-fees).toFixed(4)}</td>
                    <td class="${netClass}">${net >= 0 ? '+' : ''}${net.toFixed(4)}</td>
                </tr>`;
            }).join('');
        })
        .catch(err => console.error('Fetch error:', err));
}

// Initial load and then auto-refresh every 15s
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
            'active_trade': state.get('active_trade_id'),
            'closed_trades': state.get('closed_trades_count', 0),
            'scans': state.get('scans', 0),
        }

        opps = state.get('opportunities', [])[:10]
        opp_list = []
        for o in opps:
            vols = [v for v in (o.ex_a.volume_24h, o.ex_b.volume_24h) if v is not None]
            avg_vol = sum(vols) / len(vols) if vols else 0.0
            opp_list.append({
                'symbol': o.symbol,
                'raw_rate_a': o.raw_rate_a,
                'raw_rate_b': o.raw_rate_b,
                'raw_diff': o.raw_diff,
                'short_exchange': o.short_exchange,
                'long_exchange': o.long_exchange,
                'a_fires': o.a_fires,
                'b_fires': o.b_fires,
                'net_pct': o.net_pct,
                'price_gap_pct': o.price_gap_pct,
                'volume': avg_vol / 1e6,
                'leverage': o.leverage,
                'planned_exit': fmt_time(o.planned_exit),
                'eligible': o.eligible,
                'skip_reason': o.skip_reason,
            })

        active = state.get('active_trade')
        active_dict = None
        if active:
            active_dict = {
                'trade_id': active.trade_id,
                'symbol': active.symbol,
                'long_exchange': active.long_exchange,
                'short_exchange': active.short_exchange,
                'entry_price_long': active.entry_price_long,
                'entry_price_short': active.entry_price_short,
                'funding_collected': active.funding_collected,
                'planned_exit': fmt_time(active.planned_exit_time),
            }

        history = state.get('history', [])[-5:]
        history_list = []
        for h in history:
            hold = None
            if h.entry_time and h.exit_time:
                hold = (h.exit_time - h.entry_time).total_seconds() / 60.0
            history_list.append({
                'trade_id': h.trade_id,
                'symbol': h.symbol,
                'hold_min': hold,
                'notional': h.notional,
                'leverage': h.leverage,
                'pnl_long': h.pnl_long,
                'pnl_short': h.pnl_short,
                'funding_pnl_long': h.funding_pnl_long,
                'funding_pnl_short': h.funding_pnl_short,
                'entry_fees': h.entry_fees,
                'exit_fees': h.exit_fees,
                'total_pnl': h.total_pnl,
            })

        return jsonify({
            'summary': summary,
            'opportunities': opp_list,
            'active_trade': active_dict,
            'history': history_list,
        })

# ==============================================================================
# FUNDING BOT (with dashboard integration)
# ==============================================================================

class FundingBot:
    def __init__(self):
        self.binance = CCXTExchange("binance")
        self.bybit = CCXTExchange("bybit")
        self.scanner = Scanner([self.binance, self.bybit])

        self.active_trade: Optional[TradeRecord] = None
        self.history: List[TradeRecord] = []
        self.counter = 0
        self.cycle = 0
        self.stats = {"scans": 0, "opened": 0, "closed": 0, "fees_paid": 0.0}

        # State for dashboard
        self.dashboard_state = {
            'binance_balance': self.binance.balance,
            'bybit_balance': self.bybit.balance,
            'margin_use': 0,
            'active_trade_id': None,
            'closed_trades_count': 0,
            'scans': 0,
            'opportunities': [],
            'active_trade': None,
            'history': [],
        }

        self.dashboard = WebDashboard(self.dashboard_state)

    def _update_dashboard_state(self, opps: List[Opportunity]):
        self.dashboard_state['binance_balance'] = self.binance.balance
        self.dashboard_state['bybit_balance'] = self.bybit.balance
        margin_b = sum(p["margin"] for p in self.binance.positions.values())
        margin_y = sum(p["margin"] for p in self.bybit.positions.values())
        self.dashboard_state['margin_use'] = margin_b + margin_y
        self.dashboard_state['active_trade_id'] = self.active_trade.trade_id if self.active_trade else None
        self.dashboard_state['closed_trades_count'] = len(self.history)
        self.dashboard_state['scans'] = self.stats['scans']
        self.dashboard_state['opportunities'] = opps
        self.dashboard_state['active_trade'] = self.active_trade
        self.dashboard_state['history'] = self.history

    def _exchange_by_name(self, name: str) -> CCXTExchange:
        return self.binance if name == "binance" else self.bybit

    async def initialize(self):
        print("Loading common USDT-perpetual symbols from Binance and Bybit...")
        symbols = await self.scanner.load_symbols()
        print(f"Loaded {len(symbols)} common symbols.")
        self._load_history()

    def _load_history(self):
        path = CONFIG["CSV_FILE"]
        if not os.path.exists(path):
            return
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
                    self.history.append(trade)
                    try:
                        num = int(str(trade.trade_id).split("-")[0])
                        self.counter = max(self.counter, num + 1)
                    except (ValueError, IndexError):
                        pass
            if self.history:
                print(f"Restored {len(self.history)} closed trades from {path}.")
        except Exception as e:
            print(f"Warning: failed to load trade history from {path}: {e}")

    def _save_trade(self, trade: TradeRecord):
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
            print(f"Warning: failed to write trade to CSV: {e}")

    async def open_trade(self, opp: Opportunity) -> bool:
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
        self.counter += 1
        entry_fees = 2 * notional * CONFIG["TAKER_FEE"]
        eff_rate_long = opp.eff_rate_a if opp.ex_a.name == opp.long_exchange else opp.eff_rate_b
        eff_rate_short = opp.eff_rate_a if opp.ex_a.name == opp.short_exchange else opp.eff_rate_b
        trade = TradeRecord(
            trade_id=f"{self.counter}-{opp.symbol[:8]}",
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
        self.active_trade = trade
        self.stats["opened"] += 1
        self.stats["fees_paid"] += entry_fees
        self._save_trade(trade)
        print(f"\n>>> TRADE OPENED: {trade.trade_id} | {trade.symbol}")
        print(f"    LONG  {trade.long_exchange:8s} @ {trade.entry_price_long:.6f}")
        print(f"    SHORT {trade.short_exchange:8s} @ {trade.entry_price_short:.6f}")
        return True

    def apply_funding_if_due(self):
        t = self.active_trade
        if t is None or t.funding_collected or t.later_funding_time is None:
            return
        now = utcnow()
        if now >= t.later_funding_time:
            long_ex = self._exchange_by_name(t.long_exchange)
            short_ex = self._exchange_by_name(t.short_exchange)
            pnl_l = long_ex.apply_funding(t.pos_id_long, t.eff_rate_long)
            pnl_s = short_ex.apply_funding(t.pos_id_short, t.eff_rate_short)
            t.funding_pnl_long = pnl_l
            t.funding_pnl_short = pnl_s
            t.funding_collected = True
            print(f"\n*** Funding collected for {t.trade_id}: "
                  f"long={pnl_l:+.4f}  short={pnl_s:+.4f}  total={pnl_l + pnl_s:+.4f}")

    async def close_trade(self, reason: str):
        t = self.active_trade
        if t is None:
            return
        long_ex = self._exchange_by_name(t.long_exchange)
        short_ex = self._exchange_by_name(t.short_exchange)
        xl = await long_ex.get_exit_price(t.symbol, t.entry_price_long)
        xs = await short_ex.get_exit_price(t.symbol, t.entry_price_short)
        pnl_l, fee_l = long_ex.close_position(t.pos_id_long, xl)
        pnl_s, fee_s = short_ex.close_position(t.pos_id_short, xs)
        if pnl_l is None or pnl_s is None:
            print(f"Error closing trade {t.trade_id}: one or both positions missing.")
            self.active_trade = None
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
        self.history.append(t)
        self._save_trade(t)
        self.stats["closed"] += 1
        self.stats["fees_paid"] += exit_fees
        print(f"\n<<< TRADE CLOSED: {t.trade_id} | reason: {reason}")
        print(f"    Position PnL: long={pnl_l:+.4f}  short={pnl_s:+.4f}")
        print(f"    Funding PnL:  long={t.funding_pnl_long:+.4f}  "
              f"short={t.funding_pnl_short:+.4f}")
        print(f"    TOTAL PnL: {total_pnl:+.4f} USDT")
        self.active_trade = None

    # ------------------------------------------------------------- display (console)
    def show_top(self, opps: List[Opportunity]):
        n = CONFIG["TOP_N_DISPLAY"]
        header = (f"{'#':<3}{'Symbol':<16}{'BinR%':>8}{'BybR%':>8}{'RawD%':>8}"
                  f"{'Short':>8}{'Long':>8}{'BFire':>6}{'YFire':>6}{'Net%':>8}"
                  f"{'Gap%':>7}{'Vol(M)':>8}{'Lev':>5}{'ExitUTC':>9}  Decision")
        print(f"\n{header}")
        print("-" * 135)
        for i, o in enumerate(opps[:n], 1):
            vols = [v for v in (o.ex_a.volume_24h, o.ex_b.volume_24h) if v is not None]
            avg_vol = (sum(vols) / len(vols)) if vols else 0.0
            decision = "ELIGIBLE" if o.eligible else f"skip: {o.skip_reason}"
            print(f"{i:<3}{o.symbol:<16}{o.raw_rate_a * 100:>8.4f}{o.raw_rate_b * 100:>8.4f}"
                  f"{o.raw_diff * 100:>8.4f}{o.short_exchange:>8}{o.long_exchange:>8}"
                  f"{str(o.a_fires):>6}{str(o.b_fires):>6}{o.net_pct * 100:>8.4f}"
                  f"{o.price_gap_pct * 100:>7.3f}{avg_vol / 1e6:>8.2f}{o.leverage:>5.1f}"
                  f"{fmt_time(o.planned_exit):>9}  {decision}")

    def show_best(self, o: Opportunity):
        print(f"\n*** BEST OPPORTUNITY: {o.symbol}")
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
        print(f"    Active trade: {self.active_trade.trade_id if self.active_trade else 'None'}"
              f"   |   Closed trades: {len(self.history)}")

    def show_trade_history(self):
        recent = self.history[-5:]
        if not recent:
            return
        print(f"\n--- Recent Trade History (last {len(recent)}) ---")
        for t in recent:
            hold = "-"
            if t.entry_time and t.exit_time:
                hold = f"{(t.exit_time - t.entry_time).total_seconds() / 60.0:.1f}m"
            pos_pnl = (t.pnl_long or 0.0) + (t.pnl_short or 0.0)
            fund_pnl = t.funding_pnl_long + t.funding_pnl_short
            fees = t.entry_fees + (t.exit_fees or 0.0)
            net = t.total_pnl if t.total_pnl is not None else 0.0
            print(f"    {t.trade_id:<14}{t.symbol:<14}hold={hold:<8}"
                  f"notional=${t.notional:>8.2f}  lev={t.leverage:.1f}x  "
                  f"posPnL={pos_pnl:+.4f}  fundPnL={fund_pnl:+.4f}  "
                  f"fees={fees:.4f}  net={net:+.4f}")

    def show_active_status(self):
        t = self.active_trade
        if t is None:
            return
        now = utcnow()
        print(f"\n--- Active Trade: {t.trade_id} ({t.symbol}) ---")
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

    def _print_banner(self):
        print("=" * 90)
        print(" FUNDING RATE ARBITRAGE BOT  —  PAPER / DEMO TRADING  (with Dashboard)")
        print("=" * 90)
        print(f" Symbols tracked   : {len(self.scanner.common_symbols)}")
        print(f" Scan interval     : {CONFIG['SCAN_INTERVAL']}s")
        print(f" Demo balance      : ${CONFIG['INITIAL_BALANCE']:.2f} per exchange "
              f"(${CONFIG['INITIAL_BALANCE'] * 2:.2f} total)")
        print(f" Current balance   : Binance ${self.binance.balance:.4f}  |  "
              f"Bybit ${self.bybit.balance:.4f}")
        print(f" Max leverage      : {CONFIG['MAX_LEVERAGE']}x")
        print(f" Min 24h volume    : ${CONFIG['MIN_VOLUME_USDT']:,.0f}")
        print(f" Round-trip cost   : {CONFIG['ROUND_TRIP_COST'] * 100:.3f}%")
        print(f" Trades CSV        : {CONFIG['CSV_FILE']}")
        if CONFIG["ENABLE_WEB"]:
            print(f" Web dashboard     : http://{CONFIG['WEB_HOST']}:{CONFIG['WEB_PORT']}/")
        if CONFIG["PROXY"]:
            print(f" Proxy             : {CONFIG['PROXY']}")
        print("=" * 90)

    async def run(self):
        await self.initialize()
        self._print_banner()

        if CONFIG["ENABLE_WEB"]:
            self.dashboard.start()

        stop_event = asyncio.Event()

        def _handle_signal():
            print("\n\nShutdown signal received, closing up...")
            stop_event.set()

        loop = asyncio.get_running_loop()
        try:
            loop.add_signal_handler(signal.SIGINT, _handle_signal)
            loop.add_signal_handler(signal.SIGTERM, _handle_signal)
        except NotImplementedError:
            pass

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
        self.cycle += 1
        now = utcnow()

        print("\n" + "=" * 90)
        print(f"CYCLE {self.cycle} | {now.strftime('%Y-%m-%d %H:%M:%S UTC')} | "
              f"Binance: ${self.binance.balance:.4f} | Bybit: ${self.bybit.balance:.4f} | "
              f"Active: {self.active_trade.trade_id if self.active_trade else 'None'}")
        print("=" * 90)

        self.apply_funding_if_due()

        if (self.active_trade and self.active_trade.planned_exit_time
                and now >= self.active_trade.planned_exit_time):
            await self.close_trade("planned exit — funding collected")

        opps = await self.scanner.scan_all()
        self.stats["scans"] += 1
        best_opp = self.scanner.best(opps) if self.active_trade is None else None

        self._update_dashboard_state(opps)

        self.show_top(opps)
        if best_opp:
            self.show_best(best_opp)

        if self.active_trade is None:
            if best_opp:
                await self.open_trade(best_opp)
            else:
                print("\nNo eligible opportunity this cycle.")
        else:
            self.show_active_status()

        self.show_account_summary()
        self.show_trade_history()

    async def _shutdown(self):
        self.apply_funding_if_due()

        if self.active_trade:
            print("\nBot stopping — closing active trade...")
            try:
                await self.close_trade("bot stopped")
            except Exception:
                print("Error while closing active trade during shutdown:")
                traceback.print_exc()

        print("\n" + "=" * 90)
        print(" FINAL SUMMARY")
        print("=" * 90)
        print(f" Scans completed   : {self.stats['scans']}")
        print(f" Trades opened     : {self.stats['opened']}")
        print(f" Trades closed     : {self.stats['closed']}")
        print(f" Total fees paid   : ${self.stats['fees_paid']:.4f}")

        equity = self.binance.balance + self.bybit.balance
        initial = CONFIG["INITIAL_BALANCE"] * 2
        pnl = equity - initial
        ret_pct = (pnl / initial * 100) if initial else 0.0
        print(f" Final equity      : ${equity:.4f}")
        print(f" Final P&L         : {pnl:+.4f} USDT ({ret_pct:+.2f}%)")
        print("=" * 90)

        await self.binance.close()
        await self.bybit.close()

# ==============================================================================
# MAIN ENTRY POINT
# ==============================================================================

async def _main():
    bot = FundingBot()
    await bot.run()

def main():
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()