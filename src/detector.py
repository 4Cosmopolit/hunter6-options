import asyncio
import json
import numpy as np
from collections import deque
from sklearn.ensemble import IsolationForest
from kafka import KafkaConsumer, KafkaProducer
from prometheus_client import Counter, Gauge, start_http_server
import logging
import os

# Запуск HTTP-сервера для Prometheus
start_http_server(8000)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("detector")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
anomaly_counter = Counter('option_anomalies_total', 'Total anomalies detected', ['type'])
skew_gauge = Gauge('volatility_skew', 'Current volatility skew (put - call)')

class AdaptiveSkewDetector:
    def __init__(self, window=100, contamination=0.1):
        self.buffer = deque(maxlen=window)
        self.model = IsolationForest(contamination=contamination, random_state=42)
        self.fitted = False
        self.threshold = 0.0

    def update(self, skew_value):
        self.buffer.append(skew_value)
        skew_gauge.set(skew_value)
        if len(self.buffer) >= 50 and not self.fitted:
            X = np.array(self.buffer).reshape(-1, 1)
            self.model.fit(X)
            scores = self.model.decision_function(X)
            self.threshold = np.percentile(scores, 5)
            self.fitted = True
        elif self.fitted and len(self.buffer) % 20 == 0:
            X = np.array(self.buffer).reshape(-1, 1)
            self.model.fit(X)
        return self.predict(skew_value)

    def predict(self, skew_value):
        if not self.fitted:
            return False, 0.0
        score = self.model.decision_function([[skew_value]])[0]
        is_anomaly = score < self.threshold
        if is_anomaly:
            anomaly_counter.labels(type='skew_spike').inc()
        return is_anomaly, float(score)

async def main():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    consumer = KafkaConsumer('market_skew', bootstrap_servers=bootstrap,
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    producer = KafkaProducer(bootstrap_servers=bootstrap,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    detector = AdaptiveSkewDetector()
    for msg in consumer:
        data = msg.value
        skew = data.get('skew_value', 0.0)
        is_anomaly, score = detector.update(skew)
        if is_anomaly:
            signal = {'type': 'PUT_SKEW', 'expiry': data.get('expiry'), 'skew_value': skew, 'score': score}
            producer.send('option_signals', value=signal)
            logger.info(f"Anomaly detected: {signal}")

if __name__ == "__main__":
    asyncio.run(main())
