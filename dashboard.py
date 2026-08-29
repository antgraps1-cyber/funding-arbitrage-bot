import os
import json
from flask import Flask, jsonify, render_template_string

# ==============================================================================
# CONFIGURATION
# ==============================================================================
CONFIG = {
    "WEB_PORT": int(os.environ.get("PORT", 5000)),
    "WEB_HOST": "0.0.0.0",
}

# ==============================================================================
# WEB DASHBOARD (Standalone)
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

app = Flask(__name__)

def load_json(filepath):
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r') as f:
                return json.load(f)
        except Exception:
            pass
    return {}

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/status')
def status():
    f_state = load_json('funding_state.json')
    p_state = load_json('price_state.json')

    initial_balance = f_state.get('initial_balance', 27.48) * 2
    
    # Calculate merged equity
    bin_bal = max(f_state.get('binance_balance', 0), p_state.get('binance_balance', 0))
    byb_bal = max(f_state.get('bybit_balance', 0), p_state.get('bybit_balance', 0))
    # It might be more accurate to sum the PnLs from a base balance, but since both bots run in same accounts hypothetially, we just take max or combine them properly.
    # We will use the max for balance to avoid double counting free balance drops. 
    # Actually, they might be tracking separate balances in their memory. Just sum them?
    # No, they start with INITIAL_BALANCE. The real balance should be queried, but bots just track memory.
    
    margin_use_f = f_state.get('margin_use', 0)
    margin_use_p = p_state.get('margin_use', 0)
    
    equity = bin_bal + byb_bal + margin_use_f + margin_use_p
    pnl = equity - initial_balance
    ret_pct = (pnl / initial_balance * 100) if initial_balance else 0.0

    summary = {
        'binance_balance': bin_bal,
        'bybit_balance': byb_bal,
        'margin_use': margin_use_f + margin_use_p,
        'equity': equity,
        'pnl': pnl,
        'return_pct': ret_pct,
        'active_funding_trade': f_state.get('active_trade_id'),
        'active_price_trade': p_state.get('active_trade_id'),
        'closed_funding_trades': f_state.get('closed_trades_count', 0),
        'closed_price_trades': p_state.get('closed_trades_count', 0),
        'scans': max(f_state.get('scans', 0), p_state.get('scans', 0)),
    }

    return jsonify({
        'summary': summary,
        'funding_opportunities': f_state.get('opportunities', [])[:10],
        'price_opportunities': p_state.get('opportunities', [])[:10],
        'active_funding_trade': f_state.get('active_trade'),
        'active_price_trade': p_state.get('active_trade'),
        'funding_history': f_state.get('history', [])[-5:],
        'price_history': p_state.get('history', [])[-5:],
    })

if __name__ == "__main__":
    print(f"Web dashboard started at http://{CONFIG['WEB_HOST']}:{CONFIG['WEB_PORT']}/")
    app.run(
        host=CONFIG["WEB_HOST"],
        port=CONFIG["WEB_PORT"],
        debug=False,
        use_reloader=False,
        threaded=True
    )
