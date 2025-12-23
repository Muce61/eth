import asyncio
import os
import json
import logging
from aiohttp import web
from pathlib import Path
from datetime import datetime, timezone

# Add project root to path for imports
import sys
sys.path.append(str(Path(__file__).resolve().parent))

from data.binance_client import BinanceClient
from config.settings import Config

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DashboardServer")

class DashboardServer:
    def __init__(self):
        self.app = web.Application()
        self.client = BinanceClient()
        self.config = Config()
        self.setup_routes()
        
    def setup_routes(self):
        # Static files
        self.app.router.add_get('/', self.handle_index)
        self.app.router.add_static('/static/', path=str(Path(__file__).parent / 'static'), name='static')
        
        # API Endpoints
        self.app.router.add_get('/api/account-balance', self.get_account_balance)
        self.app.router.add_get('/api/market-status', self.get_market_status)
        self.app.router.add_get('/api/trading-stats', self.get_trading_stats)
        self.app.router.add_get('/api/current-positions', self.get_current_positions)
        self.app.router.add_get('/api/position-history', self.get_position_history)
        self.app.router.add_post('/api/refresh-history', self.refresh_history)

    async def handle_index(self, request):
        return web.FileResponse(Path(__file__).parent / 'dashboard.html')

    async def get_account_balance(self, request):
        try:
            balance = self.client.get_balance()
            return web.json_response({'balance': balance})
        except Exception as e:
            logger.error(f"获取余额时出错: {e}")
            return web.json_response({'error': str(e)}, status=500)

    async def get_market_status(self, request):
        try:
            # Fetch top gainers from binance client
            top_gainers = self.client.get_top_gainers(limit=20)
            data = []
            for symbol, ticker in top_gainers:
                data.append({
                    '币种': symbol.split('/')[0],
                    '价格': ticker['last'],
                    '涨幅%': f"{ticker['percentage']:.2f}",
                    '成交量': ticker['quoteVolume'],
                    'K线形态': 'N/A', # Placeholder for strategy logic
                    '状态': 'OK',
                    '更新时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'icon': f"/static/icons/{symbol.split('/')[0].lower()}.png"
                })
            
            return web.json_response({
                'last_updated': datetime.now(timezone.utc).timestamp(),
                'data': data
            })
        except Exception as e:
            logger.error(f"获取市场状态时出错: {e}")
            return web.json_response({'error': str(e)}, status=500)

    async def get_trading_stats(self, request):
        # Mocking stats for now or reading from trade recorder logs
        return web.json_response({
            'total_trades': 0,
            'wins': 0,
            'losses': 0,
            'win_rate': 0,
            'max_profit': 0,
            'min_profit': 0,
            'max_loss': 0,
            'min_loss': 0,
            'avg_profit': 0,
            'avg_loss': 0
        })

    async def get_current_positions(self, request):
        try:
            # Fetch private positions from binance
            # ccxt fetch_positions()
            positions = self.client.exchange.fetch_positions()
            active_positions = [p for p in positions if float(p['contracts']) > 0]
            
            p_data = []
            for pos in active_positions:
                p_data.append({
                    'symbol': pos['symbol'],
                    'side': pos['side'],
                    'size': pos['contracts'],
                    'entry_price': pos['entryPrice'],
                    'mark_price': pos['markPrice'],
                    'leverage': pos['leverage'],
                    'unrealized_pnl': f"{float(pos['unrealizedPnl']):.4f}",
                    'liquidation_price': pos['liquidationPrice']
                })
                
            return web.json_response({
                'count': len(p_data),
                'positions': p_data
            })
        except Exception as e:
            logger.error(f"获取持仓时出错: {e}")
            return web.json_response({'error': str(e)}, status=500)

    async def get_position_history(self, request):
        # Mocking or reading from trades.csv
        return web.json_response({'history': []})

    async def refresh_history(self, request):
        return web.json_response({'status': 'success', 'message': 'History refresh triggered'})

    def run(self, host='0.0.0.0', port=8080):
        logger.info(f"🚀 仪表盘服务器已启动: http://{host}:{port}")
        web.run_app(self.app, host=host, port=port)

if __name__ == '__main__':
    server = DashboardServer()
    server.run()
