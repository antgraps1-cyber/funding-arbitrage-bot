#!/usr/bin/env python3
import asyncio
import csv
import os
import signal
import sys
import threading
import traceback
import uuid
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Tuple

try:
    import ccxt.async_support as ccxt
except ImportError:
    print("This bot requires the 'ccxt' package.\nInstall it with:  pip install ccxt")
    sys.exit(1)

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
    "FETCH_TIMEOUT_SEC": 30,
    "MAX_EXIT_WAIT_MIN": 30,  # max minutes to wait for break-even after planned exit
}
CONFIG["ROUND_TRIP_COST"] = 4 * CONFIG["TAKER_FEE"] + 4 * CONFIG["SLIPPAGE"]

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

def utcnow() -> datetime: return datetime.now(timezone.utc)
def next_hour_boundary(now: datetime) -> datetime: return now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
def to_float(v, default: float = 0.0) -> float:
    try: return float(v) if v not in (None, "") else default
    except (TypeError, ValueError): return default
def parse_dt(v: Optional[str]) -> Optional[datetime]:
    if not v: return None
    try:
        dt = datetime.fromisoformat(v)
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError: return None
def fmt_time(dt: Optional[datetime]) -> str: return dt.strftime("%H:%M:%S") if dt else "-"

@dataclass
class ExchangeData:
    name: str; symbol: str; raw_rate: Optional[float]; next_funding: Optional[datetime]
    interval_min: float; price: Optional[float]; volume_24h: Optional[float]

@dataclass
class Opportunity:
    symbol: str; ex_a: ExchangeData; ex_b: ExchangeData; raw_rate_a: float; raw_rate_b: float; raw_diff: float
    eff_rate_a: float; eff_rate_b: float; a_fires: bool; b_fires: bool; short_exchange: str; long_exchange: str
    adj_diff: float; net_pct: float; price_gap_pct: float; target_ft: Optional[datetime]; later_ft: Optional[datetime]
    ttf_min: float; entry_open: Optional[datetime]; planned_exit: Optional[datetime]; leverage: float
    freq_bonus: float; timing_bonus: float; score: float; eligible: bool; skip_reason: str

@dataclass
class TradeRecord:
    trade_id: str; symbol: str; long_exchange: str; short_exchange: str; entry_time: Optional[datetime]
    entry_price_long: float; entry_price_short: float; notional: float; leverage: float; eff_rate_long: float
    eff_rate_short: float; adj_diff: float; net_pct: float; target_funding_time: Optional[datetime]
    later_funding_time: Optional[datetime]; planned_exit_time: Optional[datetime]; pos_id_long: str
    pos_id_short: str; entry_fees: float; status: str = "OPEN"; funding_collected: bool = False
    funding_pnl_long: float = 0.0; funding_pnl_short: float = 0.0; exit_time: Optional[datetime] = None
    exit_price_long: Optional[float] = None; exit_price_short: Optional[float] = None; pnl_long: Optional[float] = None
    pnl_short: Optional[float] = None; exit_fees: Optional[float] = None; total_pnl: Optional[float] = None
    funding_applied_long: bool = False; funding_applied_short: bool = False

class CCXTExchange:
    def __init__(self, name: str):
        self.name = name
        params = {"enableRateLimit": False, "options": {"defaultType": "swap"}, "timeout": CONFIG["FETCH_TIMEOUT_SEC"] * 1000}
        if CONFIG["PROXY"]: params["proxies"] = {"http": CONFIG["PROXY"], "https": CONFIG["PROXY"]}
        self.exchange = getattr(ccxt, name)(params)
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
                if attempt < CONFIG["MAX_RETRIES"]: await asyncio.sleep(CONFIG["RETRY_DELAY_SEC"])
        if not markets:
            self.symbols = {}; return {}
        result: Dict[str, float] = {}
        for symbol, market in markets.items():
            if not market.get("active", True) or market.get("quote") != "USDT" or market.get("type") != "swap" or not market.get("linear", False): continue
            base = (market.get("base") or "").upper()
            if any(kw in base for kw in CONFIG["EXCLUDE_KEYWORDS"]): continue
            result[symbol] = self._extract_interval(market)
        self.symbols = result; return result

    def _extract_interval(self, market: dict) -> float:
        info = market.get("info") or {}
        if self.name == "binance" and info.get("fundingIntervalHours"): return float(info["fundingIntervalHours"]) * 60.0
        elif self.name == "bybit" and info.get("fundingInterval"): return float(info["fundingInterval"])
        return float(CONFIG["DEFAULT_FUNDING_INTERVAL_MIN"])

    async def fetch_market_data(self) -> Tuple[dict, dict]:
        try: return await asyncio.wait_for(asyncio.gather(self.exchange.fetch_funding_rates(), self.exchange.fetch_tickers()), timeout=CONFIG["FETCH_TIMEOUT_SEC"])
        except asyncio.TimeoutError: return {}, {}

    def parse_exchange_data(self, symbol: str, fr: Optional[dict], tk: Optional[dict]) -> ExchangeData:
        r_rate = n_fund = price = volume = None
        interval_min = self.symbols.get(symbol, CONFIG["DEFAULT_FUNDING_INTERVAL_MIN"])
        if fr:
            r_rate = fr.get("fundingRate")
            if ts := fr.get("fundingTimestamp"):
                try: n_fund = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
                except: pass
        if tk:
            price = tk.get("last") or tk.get("close")
            if (volume := tk.get("quoteVolume")) is None and tk.get("baseVolume") and price:
                volume = tk.get("baseVolume") * price
        return ExchangeData(self.name, symbol, r_rate, n_fund, interval_min, price, volume)

    def open_position(self, symbol: str, side: str, price: Optional[float], notional: float, leverage: float) -> Optional[str]:
        if not price or price <= 0 or notional <= 0 or leverage <= 0: return None
        req = notional / leverage + notional * CONFIG["TAKER_FEE"] + notional * CONFIG["SLIPPAGE"] + notional * CONFIG["BUFFER_RATE"]
        if req > self.balance: return None
        self.balance -= req
        pid = str(uuid.uuid4())[:8]
        self.positions[pid] = {"symbol": symbol, "side": side, "entry_price": price * (1+CONFIG["SLIPPAGE"] if side=="buy" else 1-CONFIG["SLIPPAGE"]), "notional": notional, "margin": notional / leverage, "leverage": leverage}
        return pid

    def close_position(self, pid: str, exit_price: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
        pos = self.positions.pop(pid, None)
        if not pos or not exit_price: return None, None
        s = pos["side"]
        fill_exit = exit_price * (1-CONFIG["SLIPPAGE"] if s=="buy" else 1+CONFIG["SLIPPAGE"])
        qty = pos["notional"] / pos["entry_price"]
        pnl = (fill_exit - pos["entry_price"]) * qty if s == "buy" else (pos["entry_price"] - fill_exit) * qty
        fee = pos["notional"] * CONFIG["TAKER_FEE"]
        self.balance += pos["margin"] + pnl - fee
        return pnl, fee

    def apply_funding(self, pid: str, rate: Optional[float]) -> float:
        pos = self.positions.get(pid)
        if not pos or rate is None: return 0.0
        pnl = -pos["notional"]*rate if pos["side"]=="buy" else pos["notional"]*rate
        self.balance += pnl; return pnl

    async def get_exit_price(self, symbol: str, fallback: float) -> float:
        try:
            ticker = await self.exchange.fetch_ticker(symbol)
            return float(ticker.get("last") or ticker.get("close") or fallback)
        except: return fallback

    async def close(self):
        try: await self.exchange.close()
        except: pass

class Scanner:
    def __init__(self, exchanges: List[CCXTExchange]):
        self.exchanges = exchanges; self.common_symbols: List[str] = []
    
    async def load_symbols(self) -> List[str]:
        ssets = [set((await ex.load_symbols()).keys()) for ex in self.exchanges]
        if ssets: self.common_symbols = sorted(set.intersection(*ssets))
        return self.common_symbols

    async def scan_funding(self) -> List[Opportunity]:
        ex_a, ex_b = self.exchanges[0], self.exchanges[1]
        try: (f_a, t_a), (f_b, t_b) = await asyncio.gather(ex_a.fetch_market_data(), ex_b.fetch_market_data())
        except: return []
        if not t_a or not t_b: return []
        opps = []
        now = utcnow()
        for sym in self.common_symbols:
            if not t_a.get(sym) or not t_b.get(sym): continue
            da = ex_a.parse_exchange_data(sym, f_a.get(sym), t_a.get(sym))
            db = ex_b.parse_exchange_data(sym, f_b.get(sym), t_b.get(sym))
            if o := self._build_opportunity(da, db, now): opps.append(o)
        opps.sort(key=lambda o: o.raw_diff, reverse=True)
        return opps

    def _build_opportunity(self, a: ExchangeData, b: ExchangeData, now: datetime) -> Optional[Opportunity]:
        if a.price is None or b.price is None: return None
        if a.raw_rate is None and b.raw_rate is None: return None
        r_a, r_b = a.raw_rate or 0.0, b.raw_rate or 0.0
        hour_end, grace = next_hour_boundary(now), timedelta(seconds=CONFIG["HOUR_GRACE_SEC"])
        a_fires = bool(a.next_funding and a.next_funding <= hour_end + grace)
        b_fires = bool(b.next_funding and b.next_funding <= hour_end + grace)
        eff_a, eff_b = r_a if a_fires else 0.0, r_b if b_fires else 0.0
        raw_diff, adj_diff = abs(r_a - r_b), abs(eff_a - eff_b)
        short_ex, long_ex = (a.name, b.name) if eff_a >= eff_b else (b.name, a.name)
        avg_price = (a.price + b.price) / 2.0; gap = abs(a.price - b.price)/avg_price if avg_price else 0.0
        net_pct = raw_diff - 0.002  # net = funding diff% - 0.2%
        fts = [ft for ft in [a.next_funding if a_fires else None, b.next_funding if b_fires else None] if ft]
        if fts:
            target_ft, later_ft = min(fts), max(fts)
            ttf = (target_ft - now).total_seconds() / 60.0
            entry_open = target_ft - timedelta(minutes=CONFIG["ENTRY_WINDOW_MIN"])
            planned_exit = later_ft + timedelta(seconds=CONFIG["EXIT_DELAY_SEC"])
        else:
            target_ft = later_ft = entry_open = planned_exit = None; ttf = 9999.0
        lev = min(CONFIG["LEV_WIDE"] if gap > CONFIG["PRICE_GAP_LOW_LEV"] else CONFIG["LEV_TIGHT"], CONFIG["MAX_LEVERAGE"])
        freqs = [i for i, f in [(a.interval_min, a_fires), (b.interval_min, b_fires)] if f]
        fb = CONFIG["FREQ_BONUS"].get(int(min(freqs)), 1.0) if freqs else 1.0
        tb = 1.0 + CONFIG["TIMING_ALPHA"]/ttf if 0 < ttf < 9999 else 1.0
        score = max(net_pct, 0.0) * fb * tb
        vols = [v for v in (a.volume_24h, b.volume_24h) if v is not None]
        avg_vol = sum(vols)/len(vols) if vols else 0.0
        eligible, reasons = True, []
        if target_ft is None: eligible, reasons = False, reasons + ["no upcoming funding"]
        elif net_pct <= 0: eligible, reasons = False, reasons + ["net<=0"]
        if avg_vol < CONFIG["MIN_VOLUME_USDT"]: eligible, reasons = False, reasons + ["low volume"]
        if gap > CONFIG["PRICE_GAP_SKIP"]: eligible, reasons = False, reasons + [f"price gap {gap*100:.2f}%"]
        if 0 < ttf < CONFIG["MIN_TIME_TO_FUNDING_MIN"]: eligible, reasons = False, reasons + ["too close"]
        if ttf <= 0: eligible, reasons = False, reasons + ["funding already passed"]
        if entry_open and now < entry_open: eligible, reasons = False, reasons + ["window not open"]
        return Opportunity(a.symbol, a, b, r_a, r_b, raw_diff, eff_a, eff_b, a_fires, b_fires, short_ex, long_ex, adj_diff, net_pct, gap, target_ft, later_ft, ttf, entry_open, planned_exit, lev, fb, tb, score, eligible, "; ".join(reasons))

    @staticmethod
    def best(opportunities: List[Opportunity]) -> Optional[Opportunity]:
        eligible = sorted([o for o in opportunities if o.eligible], key=lambda x: x.score, reverse=True)
        return eligible[0] if eligible else None

class FundingBot:
    def __init__(self):
        self.binance, self.bybit = CCXTExchange("binance"), CCXTExchange("bybit")
        self.scanner = Scanner([self.binance, self.bybit])
        self.active_trade: Optional[TradeRecord] = None
        self.history: List[TradeRecord] = []
        self.counter = 0; self.scans = 0; self.opened = 0; self.closed = 0; self.fees = 0.0
        self.state_file = "funding_state.json"

    def _exchange_by_name(self, name: str) -> CCXTExchange: return self.binance if name=="binance" else self.bybit

    async def initialize(self):
        await self.scanner.load_symbols(); self._load_history()

    def _load_history(self):
        if not os.path.exists(CONFIG["CSV_FILE"]): return
        try:
            with open(CONFIG["CSV_FILE"], "r") as f:
                for row in csv.DictReader(f):
                    if row.get("status") != "CLOSED": continue
                    total_pnl = to_float(row.get("total_pnl")); half = total_pnl / 2.0
                    self.binance.balance += half; self.bybit.balance += half
                    trade = TradeRecord(
                        trade_id=row.get("trade_id", ""), symbol=row.get("symbol", ""), long_exchange=row.get("long_exchange", ""), short_exchange=row.get("short_exchange", ""),
                        entry_time=parse_dt(row.get("entry_time")), entry_price_long=to_float(row.get("entry_price_long")), entry_price_short=to_float(row.get("entry_price_short")),
                        notional=to_float(row.get("notional")), leverage=to_float(row.get("leverage")), eff_rate_long=to_float(row.get("eff_rate_long")), eff_rate_short=to_float(row.get("eff_rate_short")),
                        adj_diff=to_float(row.get("adj_diff")), net_pct=to_float(row.get("net_pct")), target_funding_time=parse_dt(row.get("target_funding_time")), later_funding_time=parse_dt(row.get("later_funding_time")), planned_exit_time=parse_dt(row.get("planned_exit_time")),
                        pos_id_long=row.get("pos_id_long", ""), pos_id_short=row.get("pos_id_short", ""), entry_fees=to_float(row.get("entry_fees")), status="CLOSED",
                        funding_collected=(row.get("funding_collected") == "True"), funding_pnl_long=to_float(row.get("funding_pnl_long")), funding_pnl_short=to_float(row.get("funding_pnl_short")),
                        exit_time=parse_dt(row.get("exit_time")), exit_price_long=to_float(row.get("exit_price_long")), exit_price_short=to_float(row.get("exit_price_short")),
                        pnl_long=to_float(row.get("pnl_long")), pnl_short=to_float(row.get("pnl_short")), exit_fees=to_float(row.get("exit_fees")), total_pnl=total_pnl
                    )
                    self.history.append(trade)
                    try: self.counter = max(self.counter, int(str(trade.trade_id).split("-")[0]) + 1)
                    except: pass
        except: pass

    def _save_trade(self, t: TradeRecord):
        p = CONFIG["CSV_FILE"]; ex = os.path.exists(p)
        row = {k: (v.isoformat() if isinstance(v, datetime) else v if v is not None else "") for k, v in asdict(t).items()}
        try:
            with open(p, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                if not ex: writer.writeheader()
                writer.writerow(row)
        except: pass

    def write_state(self, opps: List[Opportunity]):
        margin_use = sum(p["margin"] for p in self.binance.positions.values()) + sum(p["margin"] for p in self.bybit.positions.values())
        state = {
            "initial_balance": CONFIG["INITIAL_BALANCE"],
            "binance_balance": self.binance.balance,
            "bybit_balance": self.bybit.balance,
            "margin_use": margin_use,
            "scans": self.scans,
            "closed_trades_count": len(self.history),
            "active_trade_id": self.active_trade.trade_id if self.active_trade else None,
            "active_trade": asdict(self.active_trade) if self.active_trade else None,
            "opportunities": [{
                "symbol": o.symbol, "raw_rate_a": o.raw_rate_a, "raw_rate_b": o.raw_rate_b, "raw_diff": o.raw_diff,
                "short_exchange": o.short_exchange, "long_exchange": o.long_exchange, "net_pct": o.net_pct,
                "price_gap_pct": o.price_gap_pct, "eligible": o.eligible
            } for o in opps[:10]],
            "history": []
        }
        for h in self.history[-5:]:
            hold = (h.exit_time - h.entry_time).total_seconds()/60.0 if h.entry_time and h.exit_time else None
            state["history"].append({"trade_id": h.trade_id, "symbol": h.symbol, "hold_min": hold, "notional": h.notional, "total_pnl": h.total_pnl})
        
        # Serialize datetime correctly
        def dt_handler(obj):
            if isinstance(obj, datetime): return obj.isoformat()
            raise TypeError("Unknown type")
        
        try:
            with open(self.state_file, "w") as f: json.dump(state, f, default=dt_handler)
        except Exception as e: print(f"Warning: Failed to write state: {e}")

    async def open_trade(self, opp: Opportunity):
        long_ex, short_ex = self._exchange_by_name(opp.long_exchange), self._exchange_by_name(opp.short_exchange); lev = opp.leverage
        usable = min(long_ex.balance * CONFIG["CAPITAL_PCT"], short_ex.balance * CONFIG["CAPITAL_PCT"])
        if usable < 0.50: return
        notional = usable * lev
        pl, ps = (opp.ex_a.price if opp.ex_a.name == opp.long_exchange else opp.ex_b.price), (opp.ex_a.price if opp.ex_a.name == opp.short_exchange else opp.ex_b.price)
        long_id = long_ex.open_position(opp.symbol, "buy", pl, notional, lev)
        if not long_id: return
        short_id = short_ex.open_position(opp.symbol, "sell", ps, notional, lev)
        if not short_id: long_ex.close_position(long_id, pl); return
        self.counter += 1; fees = 2 * notional * CONFIG["TAKER_FEE"]
        erl = opp.eff_rate_a if opp.ex_a.name == opp.long_exchange else opp.eff_rate_b
        ers = opp.eff_rate_a if opp.ex_a.name == opp.short_exchange else opp.eff_rate_b
        self.active_trade = TradeRecord(
            trade_id=f"{self.counter}-{opp.symbol[:8]}", symbol=opp.symbol, long_exchange=opp.long_exchange, short_exchange=opp.short_exchange,
            entry_time=utcnow(), entry_price_long=pl, entry_price_short=ps, notional=notional, leverage=lev, eff_rate_long=erl, eff_rate_short=ers,
            adj_diff=opp.adj_diff, net_pct=opp.net_pct, target_funding_time=opp.target_ft, later_funding_time=opp.later_ft, planned_exit_time=opp.planned_exit,
            pos_id_long=long_id, pos_id_short=short_id, entry_fees=fees
        )
        self.opened += 1; self.fees += fees
        print(f"\n>>> FUNDING TRADE OPENED: {self.active_trade.trade_id}")

    def apply_funding(self):
        t = self.active_trade
        if not t or t.funding_collected: return
        now = utcnow()
        long_ex, short_ex = self._exchange_by_name(t.long_exchange), self._exchange_by_name(t.short_exchange)
        if not t.funding_applied_long and t.target_funding_time and now >= t.target_funding_time:
            t.funding_pnl_long = long_ex.apply_funding(t.pos_id_long, t.eff_rate_long)
            t.funding_applied_long = True
            print(f"*** Funding (long) {t.funding_pnl_long:+.4f}")
        if not t.funding_applied_short and t.later_funding_time and now >= t.later_funding_time:
            t.funding_pnl_short = short_ex.apply_funding(t.pos_id_short, t.eff_rate_short)
            t.funding_applied_short = True
            print(f"*** Funding (short) {t.funding_pnl_short:+.4f}")
        if (not t.target_funding_time or now >= t.target_funding_time) and (not t.later_funding_time or now >= t.later_funding_time):
            t.funding_collected = True; print(f"*** All funding collected for {t.trade_id}")

    async def _unrealized_price_pnl(self) -> float:
        """Calculate unrealized PnL from price movement (long + short legs)."""
        t = self.active_trade
        if not t: return 0.0
        long_ex = self._exchange_by_name(t.long_exchange)
        short_ex = self._exchange_by_name(t.short_exchange)
        cur_long = await long_ex.get_exit_price(t.symbol, t.entry_price_long)
        cur_short = await short_ex.get_exit_price(t.symbol, t.entry_price_short)
        qty_l = t.notional / t.entry_price_long
        qty_s = t.notional / t.entry_price_short
        # Long PnL: current - entry, Short PnL: entry - current
        pnl_l = (cur_long - t.entry_price_long) * qty_l
        pnl_s = (t.entry_price_short - cur_short) * qty_s
        return pnl_l + pnl_s

    async def close_trade(self, reason: str):
        t = self.active_trade
        if not t: return
        long_ex, short_ex = self._exchange_by_name(t.long_exchange), self._exchange_by_name(t.short_exchange)
        xl = await long_ex.get_exit_price(t.symbol, t.entry_price_long)
        xs = await short_ex.get_exit_price(t.symbol, t.entry_price_short)
        pnl_l, fee_l = long_ex.close_position(t.pos_id_long, xl)
        pnl_s, fee_s = short_ex.close_position(t.pos_id_short, xs)
        if pnl_l is None or pnl_s is None: self.active_trade = None; return
        t.exit_time = utcnow(); t.exit_price_long = xl; t.exit_price_short = xs
        t.pnl_long = pnl_l; t.pnl_short = pnl_s; t.exit_fees = (fee_l or 0.0) + (fee_s or 0.0)
        t.total_pnl = pnl_l + pnl_s + t.funding_pnl_long + t.funding_pnl_short - t.entry_fees - t.exit_fees
        t.status = "CLOSED"; self.history.append(t); self._save_trade(t)
        self.closed += 1; self.fees += t.exit_fees
        print(f"\n<<< FUNDING TRADE CLOSED: {t.trade_id} ({reason}) PnL: {t.total_pnl:+.4f}"); self.active_trade = None

    async def run(self):
        await self.initialize()
        print("="*60 + "\n FUNDING ARBITRAGE BOT\n" + "="*60)
        stop_ev = asyncio.Event()
        def _sig(): stop_ev.set()
        loop = asyncio.get_running_loop()
        try: loop.add_signal_handler(signal.SIGINT, _sig); loop.add_signal_handler(signal.SIGTERM, _sig)
        except: signal.signal(signal.SIGINT, lambda *a: _sig()); signal.signal(signal.SIGTERM, lambda *a: _sig())
        try:
            while not stop_ev.is_set():
                now = utcnow(); self.scans += 1
                self.apply_funding()
                if self.active_trade and self.active_trade.planned_exit_time and now >= self.active_trade.planned_exit_time:
                    # Smart exit: wait for break-even on price-diff PnL
                    price_pnl = await self._unrealized_price_pnl()
                    wait_deadline = self.active_trade.planned_exit_time + timedelta(minutes=CONFIG["MAX_EXIT_WAIT_MIN"])
                    if price_pnl >= 0:
                        await self.close_trade("planned exit (price PnL OK)")
                    elif now >= wait_deadline:
                        await self.close_trade(f"forced exit after {CONFIG['MAX_EXIT_WAIT_MIN']}min wait (price PnL: {price_pnl:+.4f})")
                    else:
                        print(f"    Waiting for break-even... price PnL: {price_pnl:+.4f} (timeout in {(wait_deadline-now).total_seconds()/60:.1f}min)")
                opps = await self.scanner.scan_funding()
                if not self.active_trade and (b := self.scanner.best(opps)): await self.open_trade(b)
                self.write_state(opps)
                try: await asyncio.wait_for(stop_ev.wait(), timeout=CONFIG["SCAN_INTERVAL"])
                except: pass
        finally:
            self.apply_funding()
            if self.active_trade: await self.close_trade("shutdown")
            await self.binance.close(); await self.bybit.close()

if __name__ == "__main__":
    asyncio.run(FundingBot().run())
