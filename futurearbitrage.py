"""
Calendar Future Arbitrage Bot
Strategy: Buy underpriced near-month futures, sell overpriced next-month futures
Uses Yahoo Finance for free market data
"""

import time
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

# ================== CONFIGURATION ==================
INITIAL_CAPITAL = 40000
TRADE_PERCENT = 0.30          # Use 30% of capital per trade
PROFIT_TARGET = 0.03          # Exit at 3% profit
COMMISSION = 0.0005           # 0.05% commission per trade

# Stock/Index Futures Symbols (Yahoo Finance format)
INSTRUMENTS = {
    'NIFTY50': '^NSEI',
    'BANKNIFTY': '^NSEBANK',
    'RELIANCE': 'RELIANCE.NS',
    'HDFCBANK': 'HDFCBANK.NS',
    'ICICIBANK': 'ICICIBANK.NS',
    'HINDUNILVR': 'HINDUNILVR.NS',
    'INFY': 'INFY.NS',
    'SBIN': 'SBIN.NS',
    'BHARTIARTL': 'BHARTIARTL.NS',
    'KOTAKBANK': 'KOTAKBANK.NS',
    'ITC': 'ITC.NS',
    'TITAN': 'TITAN.NS',
    'AXISBANK': 'AXISBANK.NS',
    'MARUTI': 'MARUTI.NS',
    'TCS': 'TCS.NS',
}

class Position:
    """Track an open position"""
    def __init__(self, symbol: str, diff_pct: float, qty: int,
                 price_near: float, price_next: float, entry_time: datetime):
        self.symbol = symbol
        self.diff_pct = diff_pct
        self.qty = qty
        self.entry_price_near = price_near
        self.entry_price_next = price_next
        self.entry_time = entry_time

    def current_pnl(self, current_near: float, current_next: float) -> float:
        """Calculate current PnL based on price difference change"""
        spread_entry = self.entry_price_next - self.entry_price_near
        spread_current = current_next - current_near
        spread_change = spread_current - spread_entry
        return spread_change * self.qty

    def exit_pnl(self, exit_near: float, exit_next: float) -> float:
        """Calculate exit PnL"""
        spread_entry = self.entry_price_next - self.entry_price_near
        spread_exit = exit_next - exit_near
        return (spread_exit - spread_entry) * self.qty


class CalendarArbitrageBot:
    """Main trading bot class"""

    def __init__(self):
        self.capital = INITIAL_CAPITAL
        self.initial_capital = INITIAL_CAPITAL
        self.positions: Dict[str, Position] = {}
        self.completed_trades = []
        self.available_funds = INITIAL_CAPITAL

    def get_futures_price(self, symbol: str) -> Optional[Tuple[float, int]]:
        """Fetch current price and volume for a futures instrument"""
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="1d", interval="1m")

            if hist.empty:
                return None

            current_price = hist['Close'].iloc[-1]
            volume = int(hist['Volume'].iloc[-1]) if not pd.isna(hist['Volume'].iloc[-1]) else 0
            return (current_price, volume)

        except Exception as e:
            return None

    def get_minute_volume(self, symbol: str) -> int:
        """Get latest 1-minute volume"""
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="5d", interval="1m")

            if hist.empty:
                return 0

            volume = int(hist['Volume'].iloc[-1]) if not pd.isna(hist['Volume'].iloc[-1]) else 0
            return volume

        except Exception:
            return 0

    def calculate_position_size(self, near_price: float, next_price: float) -> Tuple[int, float]:
        """Calculate position size based on available funds"""
        spread = abs(next_price - near_price)
        if spread == 0:
            return 0, 0.0

        margin_requirement = spread
        trade_value = self.available_funds * TRADE_PERCENT
        qty = int(trade_value / margin_requirement)
        qty = max(1, min(qty, 100))  # Cap at 100 lots for safety
        required_margin = qty * margin_requirement

        if required_margin > self.available_funds:
            qty = int(self.available_funds / margin_requirement)
            required_margin = qty * margin_requirement

        return qty, required_margin

    def check_volume_requirement(self, symbol: str, qty: int) -> bool:
        """Check if 1-min volume >= 150 * position size"""
        minute_vol = self.get_minute_volume(symbol)
        required_vol = qty * 150
        return minute_vol >= required_vol

    def scan_opportunities(self) -> List[Tuple]:
        """Scan all instruments and return best opportunities"""
        opportunities = []

        print(f"\n{'='*70}")
        print(f"SCAN @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='<70}")

        for name, symbol in INSTRUMENTS.items():
            try:
                data = self.get_futures_price(symbol)
                if not data:
                    continue

                current_price, _ = data

                # Simulate near and next month futures prices
                # In production, fetch actual futures data
                near_price = current_price
                next_price = current_price * (1 + np.random.uniform(-0.02, 0.03))

                diff_pct = abs((next_price - near_price) / near_price) * 100

                if diff_pct > 0:
                    opportunities.append((
                        name,
                        near_price,
                        next_price,
                        diff_pct,
                        current_price
                    ))

            except Exception as e:
                continue

        opportunities.sort(key=lambda x: x[3], reverse=True)
        return opportunities[:10]

    def execute_trade(self, opp: Tuple) -> bool:
        """Execute a calendar spread trade"""
        name, near_price, next_price, diff_pct, _ = opp

        if name in self.positions:
            return False

        qty, required_margin = self.calculate_position_size(near_price, next_price)

        if qty <= 0:
            return False

        if not self.check_volume_requirement(INSTRUMENTS[name], qty):
            print(f"Volume insufficient for {name}")
            return False

        # Execute trade
        self.positions[name] = Position(
            symbol=name,
            diff_pct=diff_pct,
            qty=qty,
            price_near=near_price,
            price_next=next_price,
            entry_time=datetime.now()
        )

        self.available_funds -= required_margin

        print(f"\n{'='*60}")
        print(f"TRADE EXECUTED: {name}")
        print(f"Diff: {diff_pct:.2f}% | Qty: {qty}")
        print(f"Margin Used: ₹{required_margin:,.2f}")
        print(f"Remaining Funds: ₹{self.available_funds:,.2f}")
        print(f"{'='*60}\n")

        return True

    def check_and_exit_positions(self) -> None:
        """Check all positions and exit if profit target reached"""
        if not self.positions:
            return

        to_exit = []

        for symbol, pos in self.positions.items():
            try:
                data = self.get_futures_price(INSTRUMENTS[symbol])
                if not data:
                    continue

                current_price, _ = data
                near_price = current_price
                next_price = current_price * (1 + np.random.uniform(-0.01, 0.02))

                pnl = pos.exit_pnl(near_price, next_price)
                pnl_pct = (pnl / (pos.qty * pos.entry_price_near)) * 100

                print(f"[{symbol}] Entry: {pos.entry_price_near:.2f} | "
                      f"Current: {current_price:.2f} | PnL: {pnl_pct:+.2f}%")

                if pnl_pct >= PROFIT_TARGET:
                    to_exit.append((symbol, pnl, near_price, next_price))

            except Exception as e:
                continue

        for symbol, pnl, exit_near, exit_next in to_exit:
            pos = self.positions[symbol]
            self.available_funds += (pos.qty * abs(pos.entry_price_next - pos.entry_price_near))

            self.completed_trades.append({
                'symbol': symbol,
                'entry_time': pos.entry_time,
                'exit_time': datetime.now(),
                'qty': pos.qty,
                'entry_diff': pos.diff_pct,
                'exit_pnl': pnl,
            })

            self.capital += pnl

            print(f"\n{'='*60}")
            print(f"EXIT: {symbol} - Profit Target {PROFIT_TARGET*100:.0f}% Reached")
            print(f"PnL: ₹{pnl:+,.2f} | Capital: ₹{self.capital:,.2f}")
            print(f"{'='*60}\n")

            del self.positions[symbol]

    def print_status(self) -> None:
        """Print current bot status"""
        print(f"\n{'~'*70}")
        print(f"STATUS @ {datetime.now().strftime('%H:%M:%S')}")
        print(f"Capital: ₹{self.capital:,.2f} | Available: ₹{self.available_funds:,.2f}")
        print(f"Open Positions: {len(self.positions)} | Total Trades: {len(self.completed_trades)}")

        if self.positions:
            print("\nOpen Positions:")
            for symbol, pos in self.positions.items():
                print(f"  - {symbol}: qty={pos.qty}, entry_diff={pos.diff_pct:.2f}%")

        net_pnl = self.capital - self.initial_capital
        print(f"\nNet PnL: ₹{net_pnl:+,.2f} ({net_pnl/self.initial_capital*100:+.2f}%)")
        print(f"{'~'*70}")

    def run(self) -> None:
        """Main bot loop"""
        print(f"\n{'#'*70}")
        print(f"CALENDAR FUTURES ARBITRAGE BOT")
        print(f"Initial Capital: ₹{INITIAL_CAPITAL:,.2f}")
        print(f"Profit Target: {PROFIT_TARGET*100:.0f}%")
        print(f"Scan Interval: 1 minute")
        print(f"{'#'*70}\n")

        scan_count = 0
        last_opportunities = []

        while True:
            scan_count += 1

            opportunities = self.scan_opportunities()
            last_opportunities = opportunities

            # Show best opportunities
            if opportunities:
                print("\nTop 10 Opportunities:")
                for i, (name, near, next_, diff, _) in enumerate(opportunities[:10], 1):
                    print(f"{i:2}. {name:15} Diff: {diff:6.2f}% "
                          f"| Near: {near:8.2f} | Next: {next_:8.2f}")
            else:
                print("No opportunities found")

            # Check if we should trade any opportunity
            if len(self.positions) == 0 and opportunities:
                best_opp = opportunities[0]
                if best_opp[3] > 0.5:
                    self.execute_trade(best_opp)

            # Check existing positions
            self.check_and_exit_positions()

            # Print status
            self.print_status()

            print(f"\nNext scan in 60 seconds... (scan #{scan_count})")
            time.sleep(60)


if __name__ == "__main__":
    bot = CalendarArbitrageBot()
    try:
        bot.run()
    except KeyboardInterrupt:
        print("\n\nBot stopped by user")
    except Exception as e:
        print(f"\nError: {e}")