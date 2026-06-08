# Hunter 6.0 — Autonomous Options Trading with Delta Hedging

Полностью автономная мультиагентная система для торговли опционами на Bybit с динамическим хеджированием дельты. Обнаруживает аномалии в улыбке волатильности, автоматически выставляет Long Strangle, хеджирует дельту фьючерсами и управляет позицией по Take Profit / Stop Loss. Самообучается на потоковых данных через адаптивные пороги Isolation Forest.

## ✨ Ключевые возможности

- **Адаптивный детектор аномалий** — Isolation Forest с онлайн-обновлением порогов
- **Автоматический Position Builder** — комбинированные ордера (Long Strangle) с реальными ценами опционов
- **Динамическое хеджирование дельты** — подписка на WebSocket позиций, автоматическая ребалансировка фьючерсами
- **Управление позицией** — Take Profit (+50%), Stop Loss (-30%), настраиваемые параметры
- **Промышленная надёжность** — Kafka (A2A-шина), Prometheus + Grafana, отказоустойчивость
- **Полная автономность** — запуск одной командой, без ручного вмешательства

## 🎯 Торговая стратегия

**Volatility Skew Arbitrage (Long Strangle)**

| Параметр | Значение |
|----------|----------|
| Вход | PUT_SKEW или CALL_SKEW (аномалия в улыбке волатильности) |
| Инструмент | OTM Put + OTM Call (Long Strangle) |
| Take Profit | +50% от стоимости входа |
| Stop Loss | -30% от стоимости входа |
| Хеджирование | Динамическое дельта-хеджирование фьючерсами BTCUSDT |
| Риск-менеджмент | Размер позиции ≤5% депозита, дневной лимит убытка 3% |

## 🧠 Архитектура

market_skew (Kafka)
│
▼
┌─────────────┐ ┌──────────────┐ ┌─────────────┐
│ detector │────▶│ executor │────▶│ hedger │
│ (Isolation │ │ (Long Strangle)│ │ (Delta Hedge)│
│ Forest) │ │ │ │ │
└─────────────┘ └──────┬───────┘ └─────────────┘
│
▼
┌──────────────┐
│ position │
│ manager │
│ (TP/SL) │
└──────────────┘


- **A2A-шина**: Apache Kafka
- **Исполнение**: Bybit V5 API (опционы и фьючерсы)
- **Мониторинг**: Prometheus + Grafana
- **Самообучение**: адаптивные пороги детектора

## 🚀 Быстрый старт

```bash
# 1. Клонируйте репозиторий
git clone https://github.com/4Cosmopolit/hunter6-options.git
cd hunter6-options

# 2. Настройте переменные окружения
cp .env.example .env
nano .env   # вставьте ключи Bybit Testnet

# 3. Запустите систему
docker-compose up --build -d

Grafana: http://localhost:3000 (admin/admin)

⚙️ Переменные окружения (.env)
Переменная	Описание	По умолчанию
BYBIT_API_KEY	API-ключ Bybit (Testnet)	—
BYBIT_API_SECRET	Секретный ключ	—
TESTNET	Использовать тестовую сеть	true
POSITION_SIZE_USDT	Размер позиции в USDT	1000
TAKE_PROFIT_PCT	Процент Take Profit	50
STOP_LOSS_PCT	Процент Stop Loss	30
MAX_DAILY_LOSS_PCT	Дневной лимит убытка	3
DELTA_THRESHOLD	Порог для хеджирования дельты	0.1
📊 Мониторинг
Метрики Prometheus
Метрика	Описание
option_total_delta	Суммарная дельта портфеля
volatility_skew	Текущий перекос волатильности
option_anomalies_total	Количество обнаруженных аномалий
orders_placed_total	Количество выставленных ордеров
hedge_trades_total	Количество хеджирующих сделок
strangle_unrealized_pnl	Нереализованная прибыль позиции
Порты метрик
Сервис	Порт
detector	8000
executor	8001
hedger	8002
🧪 Тестирование на Testnet
Убедитесь, что все контейнеры запущены:

bash
docker-compose ps
Отправьте тестовый сигнал в Kafka:

bash
docker exec -it hunter6-options-kafka-1 bash
kafka-console-producer --broker-list localhost:9092 --topic market_skew
# Вставьте JSON и нажмите Enter:
{"skew": 0.15, "expiry": "2025-01-31", "timestamp": 1735689600}
# Ctrl+C для выхода
Проверьте логи:

bash
docker-compose logs -f detector
docker-compose logs -f executor
docker-compose logs -f hedger
🛠️ Требования
Docker + Docker Compose

4+ CPU ядер, 8+ GB RAM

📄 Лицензия
MIT
