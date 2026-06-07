from prometheus_client import Counter, Gauge, Histogram

# Греки портфеля
option_delta = Gauge('option_total_delta', 'Total delta of option portfolio')
option_gamma = Gauge('option_total_gamma', 'Total gamma')
option_theta = Gauge('option_total_theta', 'Total theta')
option_vega = Gauge('option_total_vega', 'Total vega')
pnl_unrealized = Gauge('pnl_unrealized', 'Unrealized PnL')
pnl_realized = Gauge('pnl_realized', 'Realized PnL')

# Счетчики исполнения
order_counter = Counter('orders_placed_total', 'Total orders placed', ['type'])
order_failure_counter = Counter('order_failures_total', 'Order failures', ['reason'])
hedge_trade_counter = Counter('hedge_trades_total', 'Hedge trades executed')

# Детектор аномалий
anomaly_counter = Counter('option_anomalies_total', 'Volatility skew anomalies', ['type'])
skew_gauge = Gauge('volatility_skew', 'Current volatility skew (put - call)')
