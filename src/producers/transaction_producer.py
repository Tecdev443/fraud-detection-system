import json
import time
import random
from datetime import datetime, timedelta
from confluent_kafka import Producer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = 'localhost:9092'
TOPIC = 'transactions'

producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})

MERCHANTS = ['amazon', 'walmart', 'target', 'costco', 'kroger', 'home_depot']
COUNTRIES = ['US', 'GB', 'CA', 'DE', 'FR', 'AU', 'NZ', 'SG']
CARD_TYPES = ['VISA', 'MASTERCARD', 'AMEX', 'DISCOVER']
DEVICE_TYPES = ['mobile', 'desktop', 'tablet', 'atm']

def generate_transaction():
    user_id = f"user_{random.randint(1, 50000)}"
    
    # Fraudulent transactions (5%)
    is_fraud = random.random() < 0.05
    
    if is_fraud:
        amount = random.uniform(500, 5000)
        country = random.choice(['NG', 'RU', 'CN', 'KP'])  # high-fraud countries
        velocity = random.randint(5, 10)  # transactions per hour
    else:
        amount = random.lognormvariate(3.5, 1.2)
        country = random.choice(COUNTRIES)
        velocity = random.randint(0, 2)
    
    return {
        'transaction_id': f'txn_{int(time.time() * 1000)}_{random.randint(0, 9999)}',
        'user_id': user_id,
        'amount': round(amount, 2),
        'currency': 'USD',
        'merchant': random.choice(MERCHANTS),
        'card_country': country,
        'card_type': random.choice(CARD_TYPES),
        'device_type': random.choice(DEVICE_TYPES),
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'user_age': random.randint(18, 80),
        'account_age_days': random.randint(1, 3650),
        'previous_txn_count': random.randint(0, 1000),
        'is_fraud_label': is_fraud
    }

def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.debug(f'Message delivered: {msg.topic()} [{msg.partition()}]')

if __name__ == '__main__':
    try:
        logger.info('Starting transaction producer...')
        txn_count = 0
        while True:
            txn = generate_transaction()
            producer.produce(
                TOPIC,
                key=txn['user_id'].encode(),
                value=json.dumps(txn).encode(),
                callback=delivery_report
            )
            producer.poll(0)
            txn_count += 1
            
            if txn_count % 100 == 0:
                logger.info(f'Produced {txn_count} transactions')
            
            time.sleep(0.01)  # ~100 events/sec
    except KeyboardInterrupt:
        logger.info('Producer shutting down')
    finally:
        producer.flush()
