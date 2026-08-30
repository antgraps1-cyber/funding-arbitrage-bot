#!/usr/bin/env python3
import asyncio
import csv
import os
import signal
import sys
import uuid
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional, Dict, List, Tuple

try:
    import ccxt.async_support as ccxt
except ImportError:
    print("This bot requires the 'ccxt' package.\nInstall it with:  pip install ccxt")
    sys.exit(1)

CONFIG = {
    "PRICE_ARB_ENABLED": True,
    "PRICE_ARB_MIN_NET_PROFIT_PCT": 0.001,      # 0.1% min net profit to execute
    "PRICE_ARB_CAPITAL_PCT": 0.5,             # use 50% of free balance per trade
    "PRICE_ARB_MIN_VOLUME_USDT": 1_000_000,
    "PRICE_ARB_CSV_FILE": "price_trades.csv",
    "TAKER_FEE": 0.001,                       # 0.1% spot taker fee typical for Binance/Bybit
    "TRANSFER_FEE_PCT": 0.005,                # 0.5% constant transfer fee for all coins
    "INITIAL_BALANCE": 27.48,
    "SCAN_INTERVAL": 60,
    "PROXY": None,
    "EXCLUDE_KEYWORDS": ("UP", "DOWN", "BEAR", "BULL", "3L", "3S"),
    "MAX_RETRIES": 3,
    "RETRY_DELAY_SEC": 3,
    "FETCH_TIMEOUT_SEC": 30,
}

PRICE_CSV_FIELDS = [
    "trade_id", "symbol", "buy_exchange", "sell_exchange",
    "entry_time", "entry_price_buy", "entry_price_sell",
    "notional", "net_profit_pct", "price_gap",
    "total_fees", "status", "total_pnl"
]

def utcnow() -> datetime: return datetime.now(timezone.utc)
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

@dataclass
class PriceOpportunity:
    symbol: str; buy_exchange: str; sell_exchange: str; buy_price: float; sell_price: float
    price_gap_pct: float; net_profit_pct: float; volume: float; eligible: bool; skip_reason: str

@dataclass
class PriceTradeRecord:
    trade_id: str; symbol: str; buy_exchange: str; sell_exchange: str; entry_time: Optional[datetime]
    entry_price_buy: float; entry_price_sell: float; notional: float; net_profit_pct: float; price_gap: float
    total_fees: float; status: str = "CLOSED"; total_pnl: Optional[float] = None
    # Add dummy exit_time for dashboard compatibility
    exit_time: Optional[datetime] = None

class CCXTExchange:
    def __init__(self, name: str):
        self.name = name
        params = {"enableRateLimit": False, "options": {"defaultType": "spot"}, "timeout": CONFIG["FETCH_TIMEOUT_SEC"] * 1000}
        if CONFIG["PROXY"]: params["proxies"] = {"http": CONFIG["PROXY"], "https": CONFIG["PROXY"]}
        self.exchange = getattr(ccxt, name)(params)
        self.balance: float = CONFIG["INITIAL_BALANCE"]
        self.symbols: List[str] = []

    async def load_symbols(self) -> List[str]:
        markets = None
        for attempt in range(1, CONFIG["MAX_RETRIES"] + 1):
            try:
                markets = await self.exchange.load_markets()
                break
            except Exception as e:
                if attempt < CONFIG["MAX_RETRIES"]: await asyncio.sleep(CONFIG["RETRY_DELAY_SEC"])
        if not markets:
            self.symbols = []; return []
        result = []
        for symbol, market in markets.items():
            if not market.get("active", True) or market.get("quote") != "USDT" or market.get("type") != "spot": continue
            base = (market.get("base") or "").upper()
            if any(kw in base for kw in CONFIG["EXCLUDE_KEYWORDS"]): continue
            result.append(symbol)
        self.symbols = result; return result

    async def fetch_tickers(self) -> dict:
        try: return await asyncio.wait_for(self.exchange.fetch_tickers(), timeout=CONFIG["FETCH_TIMEOUT_SEC"])
        except asyncio.TimeoutError: return {}

    async def close(self):
        try: await self.exchange.close()
        except: pass

class Scanner:
    def __init__(self, exchanges: List[CCXTExchange]):
        self.exchanges = exchanges; self.common_symbols: List[str] = []
    
    async def load_symbols(self) -> List[str]:
        ssets = [set((await ex.load_symbols())) for ex in self.exchanges]
        if ssets: self.common_symbols = sorted(set.intersection(*ssets))
        return self.common_symbols

    async def scan_price(self) -> List[PriceOpportunity]:
        ex_a, ex_b = self.exchanges[0], self.exchanges[1]
        try: t_a, t_b = await asyncio.gather(ex_a.fetch_tickers(), ex_b.fetch_tickers())
        except: return []
        if not t_a or not t_b: return []
        
        opps = []
        usdt_notional = CONFIG["INITIAL_BALANCE"] * CONFIG["PRICE_ARB_CAPITAL_PCT"]
        transfer_fee_pct = CONFIG["TRANSFER_FEE_PCT"]
        
        for sym in self.common_symbols:
            ta, tb = t_a.get(sym), t_b.get(sym)
            if not ta or not tb: continue
            pa, pb = to_float(ta.get("last") or ta.get("close")), to_float(tb.get("last") or tb.get("close"))
            if not pa or not pb: continue
            
            bx, sx, bp, sp = (ex_a.name, ex_b.name, pa, pb) if pa < pb else (ex_b.name, ex_a.name, pb, pa)
            
            # Gap % calculation for spatial: (sell - buy) / buy
            gap = (sp - bp) / bp
            
            # Net profit % = Gap - 0.6% (0.006)
            net_profit_pct = gap - 0.006
            
            va = to_float(ta.get("quoteVolume"))
            if not va:
                bv_a = to_float(ta.get("baseVolume"))
                va = bv_a * pa if bv_a else 0.0
            vb = to_float(tb.get("quoteVolume"))
            if not vb:
                bv_b = to_float(tb.get("baseVolume"))
                vb = bv_b * pb if bv_b else 0.0
            mvol = min(va, vb)
            
            eligible, reasons = True, []
            if net_profit_pct < CONFIG["PRICE_ARB_MIN_NET_PROFIT_PCT"]: 
                eligible, reasons = False, reasons + [f"net<{CONFIG['PRICE_ARB_MIN_NET_PROFIT_PCT']*100:.2f}%"]
            if mvol < CONFIG["PRICE_ARB_MIN_VOLUME_USDT"]: 
                eligible, reasons = False, reasons + ["low volume"]
                
            opps.append(PriceOpportunity(
                sym, bx, sx, bp, sp, gap, net_profit_pct, mvol, eligible, "; ".join(reasons)
            ))
            
        opps.sort(key=lambda o: o.net_profit_pct, reverse=True)
        return opps

    @staticmethod
    def best(opps: List[PriceOpportunity]) -> Optional[PriceOpportunity]:
        eligible = [o for o in opps if o.eligible]
        return eligible[0] if eligible else None

class PriceBot:
    def __init__(self):
        self.binance, self.bybit = CCXTExchange("binance"), CCXTExchange("bybit")
        self.scanner = Scanner([self.binance, self.bybit])
        self.history: List[PriceTradeRecord] = []
        self.counter = 0; self.scans = 0; self.trades = 0; self.fees = 0.0
        self.state_file = "price_state.json"

    def _exchange_by_name(self, name: str) -> CCXTExchange: return self.binance if name=="binance" else self.bybit

    async def initialize(self):
        await self.scanner.load_symbols(); self._load_history()

    def _load_history(self):
        if not os.path.exists(CONFIG["PRICE_ARB_CSV_FILE"]): return
        try:
            with open(CONFIG["PRICE_ARB_CSV_FILE"], "r") as f:
                for row in csv.DictReader(f):
                    if row.get("status") != "CLOSED": continue
                    # Reconstruct simple history
                    trade = PriceTradeRecord(
                        trade_id=row.get("trade_id", ""), symbol=row.get("symbol", ""), buy_exchange=row.get("buy_exchange", ""), sell_exchange=row.get("sell_exchange", ""),
                        entry_time=parse_dt(row.get("entry_time")), entry_price_buy=to_float(row.get("entry_price_buy")), entry_price_sell=to_float(row.get("entry_price_sell")),
                        notional=to_float(row.get("notional")), net_profit_pct=to_float(row.get("net_profit_pct")), price_gap=to_float(row.get("price_gap")),
                        total_fees=to_float(row.get("total_fees")), status="CLOSED", total_pnl=to_float(row.get("total_pnl")), exit_time=parse_dt(row.get("entry_time"))
                    )
                    self.history.append(trade)
                    try: self.counter = max(self.counter, int(str(trade.trade_id).replace("S","").split("-")[0]) + 1)
                    except: pass
        except: pass

    def _save_trade(self, t: PriceTradeRecord):
        p = CONFIG["PRICE_ARB_CSV_FILE"]; ex = os.path.exists(p)
        row = {k: (v.isoformat() if isinstance(v, datetime) else v if v is not None else "") for k, v in asdict(t).items()}
        try:
            with open(p, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=PRICE_CSV_FIELDS)
                if not ex: writer.writeheader()
                writer.writerow(row)
        except: pass

    def write_state(self, opps: List[PriceOpportunity]):
        state = {
            "initial_balance": CONFIG["INITIAL_BALANCE"],
            "binance_balance": self.binance.balance,
            "bybit_balance": self.bybit.balance,
            "margin_use": 0.0, # Spatial arb does not hold margin
            "scans": self.scans,
            "closed_trades_count": len(self.history),
            "active_trade_id": None,
            "active_trade": None,
            "opportunities": [{
                "symbol": o.symbol, "buy_exchange": o.buy_exchange, "sell_exchange": o.sell_exchange,
                "buy_price": o.buy_price, "sell_price": o.sell_price, "price_gap_pct": o.price_gap_pct,
                "net_profit_pct": o.net_profit_pct,
                "volume": o.volume, "eligible": o.eligible, "skip_reason": o.skip_reason
            } for o in opps[:10]],
            "history": []
        }
        for h in self.history[-5:]:
            state["history"].append({"trade_id": h.trade_id, "symbol": h.symbol, "hold_min": 0, "notional": h.notional, "total_pnl": h.total_pnl})
        
        def dt_handler(obj):
            if isinstance(obj, datetime): return obj.isoformat()
            raise TypeError("Unknown type")
        
        try:
            with open(self.state_file, "w") as f: json.dump(state, f, default=dt_handler)
        except Exception as e: print(f"Warning: Failed to write state: {e}")

    async def execute_spatial_arbitrage(self, opp: PriceOpportunity):
        bx, sx = self._exchange_by_name(opp.buy_exchange), self._exchange_by_name(opp.sell_exchange)
        usable = bx.balance * CONFIG["PRICE_ARB_CAPITAL_PCT"]
        if usable < 1.0: 
            print(f"Skipped {opp.symbol} - insufficient usable balance on {bx.name} (${usable:.4f})")
            return
            
        buy_notional = usable
        buy_fee = buy_notional * CONFIG["TAKER_FEE"]
        net_buy_usdt = buy_notional - buy_fee
        coins_bought = net_buy_usdt / opp.buy_price
        
        # Deduct purchase from buy exchange
        bx.balance -= buy_notional
        
        # Simulate transfer fee (deducted before selling)
        transfer_fee = net_buy_usdt * CONFIG["TRANSFER_FEE_PCT"]
        
        # Sell on sx
        gross_sell_usdt = coins_bought * opp.sell_price
        sell_fee = gross_sell_usdt * CONFIG["TAKER_FEE"]
        net_sell_usdt = gross_sell_usdt - sell_fee
        
        # Add proceeds to sell exchange (minus transfer fee cost)
        sx.balance += (net_sell_usdt - transfer_fee)
        
        total_fees = buy_fee + sell_fee + transfer_fee
        total_pnl = (net_sell_usdt - transfer_fee) - buy_notional
        
        self.counter += 1
        trade = PriceTradeRecord(
            trade_id=f"S{self.counter}-{opp.symbol[:8]}", symbol=opp.symbol, 
            buy_exchange=opp.buy_exchange, sell_exchange=opp.sell_exchange,
            entry_time=utcnow(), exit_time=utcnow(), 
            entry_price_buy=opp.buy_price, entry_price_sell=opp.sell_price, 
            notional=buy_notional, net_profit_pct=opp.net_profit_pct, price_gap=opp.price_gap_pct,
            total_fees=total_fees, status="CLOSED", total_pnl=total_pnl
        )
        self.trades += 1
        self.fees += total_fees
        self.history.append(trade)
        self._save_trade(trade)
        
        print(f"\n>>> INSTANT SPATIAL TRADE EXECUTED: {trade.trade_id}")
        print(f"    Bought {coins_bought:.6f} {opp.symbol} on {bx.name} for ${buy_notional:.4f}")
        print(f"    Sold on {sx.name} | Total Fees: ${total_fees:.4f} | PNL: ${total_pnl:.4f}")

    async def run(self):
        await self.initialize()
        print("="*60 + "\n SPOT SPATIAL ARBITRAGE BOT\n" + "="*60)
        stop_ev = asyncio.Event()
        def _sig(): stop_ev.set()
        loop = asyncio.get_running_loop()
        try: loop.add_signal_handler(signal.SIGINT, _sig); loop.add_signal_handler(signal.SIGTERM, _sig)
        except: signal.signal(signal.SIGINT, lambda *a: _sig()); signal.signal(signal.SIGTERM, lambda *a: _sig())
        try:
            while not stop_ev.is_set():
                now = utcnow(); self.scans += 1
                opps = await self.scanner.scan_price()
                b = self.scanner.best(opps)
                if b: 
                    await self.execute_spatial_arbitrage(b)
                self.write_state(opps)
                try: await asyncio.wait_for(stop_ev.wait(), timeout=CONFIG["SCAN_INTERVAL"])
                except: pass
        finally:
            await self.binance.close(); await self.bybit.close()

if __name__ == "__main__":
    asyncio.run(PriceBot().run())
