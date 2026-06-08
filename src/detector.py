# --- В функции main() детектора ---
async def main():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    consumer = KafkaConsumer('market_skew', bootstrap_servers=bootstrap,
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    producer = KafkaProducer(bootstrap_servers=bootstrap,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    detector = AdaptiveSkewDetector()
    logger.info("Detector started, waiting for market data...")

    for msg in consumer:
        data = msg.value
        if 'skew' in data:
            skew_value = data['skew']
            current_price = data.get('price', 60000)  # Получаем цену из Kafka
            is_anomaly, score = detector.update(skew_value)

            if is_anomaly:
                # Адаптивный расчет страйков на основе силы аномалии и цены
                # При сильном перекосе (|skew| > 0.8) выбираем более дальние страйки
                magnitude = abs(skew_value)
                if magnitude > 0.8:
                    put_offset_pct = 0.1  # 10% от цены
                    call_offset_pct = 0.15 # 15% от цены
                else:
                    put_offset_pct = 0.05
                    call_offset_pct = 0.07

                put_strike = int(current_price * (1 - put_offset_pct))
                call_strike = int(current_price * (1 + call_offset_pct))

                signal = {
                    'type': 'PUT_SKEW' if skew_value > 0 else 'CALL_SKEW',
                    'expiry': data.get('expiry'),
                    'skew_value': skew_value,
                    'score': score,
                    'put_strike': put_strike,
                    'call_strike': call_strike,
                    'current_price': current_price,
                    'timestamp': data['timestamp']
                }
                producer.send('option_signals', value=signal)
                logger.info(f"Signal sent: {signal['type']} with put={put_strike}, call={call_strike}")
