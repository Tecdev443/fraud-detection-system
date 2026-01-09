# Real-Time Fraud Detection & Event Streaming Platform

Enterprise-grade streaming architecture for real-time fraud detection using Apache Kafka, Spark Structured Streaming, and machine learning.

## Architecture

- **Ingestion**: Kafka producers ingest transaction events (10k-100k events/sec)
- **Stream Processing**: PySpark Structured Streaming for real-time feature engineering and fraud scoring
- **Serving Store**: Cassandra for sub-100ms fraud decision queries
- **Analytics**: Snowflake for historical analysis and model retraining data
- **Monitoring**: Prometheus metrics + structured logging

## Tech Stack

- Apache Kafka (event backbone)
- PySpark 3.3+ (streaming engine)
- Cassandra 3.11+ (serving store)
- Snowflake (analytics warehouse)
- Redis (feature cache)
- Docker Compose (local stack)

## Project Structure

```
fraud-detection-platform/
â”œâ”€â”€ src/
â”‚   â”œâ”€â”€ producers/          # Kafka producers
â”‚   â”‚   â”œâ”€â”€ transaction_producer.py
â”‚   â”‚   â””â”€â”€ synthetic_generator.py
â”‚   â”œâ”€â”€ processors/         # Stream processing jobs
â”‚   â”‚   â”œâ”€â”€ fraud_scoring_job.py
â”‚   â”‚   â””â”€â”€ feature_aggregator.py
â”‚   â”œâ”€â”€ models/             # ML models
â”‚   â”‚   â”œâ”€â”€ fraud_model.pkl
â”‚   â”‚   â””â”€â”€ feature_extractor.py
â”‚   â”œâ”€â”€ storage/            # Database connectors
â”‚   â”‚   â”œâ”€â”€ cassandra_sink.py
â”‚   â”‚   â””â”€â”€ snowflake_sink.py
â”‚   â””â”€â”€ utils/              # Helper functions
â”‚       â”œâ”€â”€ config.py
â”‚       â”œâ”€â”€ logging_setup.py
â”‚       â””â”€â”€ metrics.py
â”œâ”€â”€ tests/
â”‚   â”œâ”€â”€ test_producer.py
â”‚   â”œâ”€â”€ test_processors.py
â”‚   â””â”€â”€ test_fraud_model.py
â”œâ”€â”€ docker-compose.yml      # Local stack (Kafka, Cassandra, Redis)
â”œâ”€â”€ Dockerfile
â”œâ”€â”€ requirements.txt
â”œâ”€â”€ setup.py
â”œâ”€â”€ config.yaml             # Configuration
â””â”€â”€ README.md
```

## Quick Start

### 1. Start Local Stack
```bash
docker-compose up -d
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Run Producer
```bash
python src/producers/transaction_producer.py
```

### 4. Run Spark Streaming Job
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 \
  --master local[4] \
  src/processors/fraud_scoring_job.py
```

## Features

- **Real-Time Scoring**: Sub-second fraud decision latency
- **Feature Engineering**: 50+ computed features (velocity, geographic, behavioral)
- **Model Management**: Version-controlled ML models with A/B testing
- **High Throughput**: 100k+ events/sec on modest hardware
- **Fault Tolerance**: Exactly-once semantics with Kafka transactions
- **Monitoring**: Prometheus metrics for all components

## Configuration

Edit `config.yaml` to customize:
- Kafka bootstrap servers
- Cassandra contact points
- Snowflake credentials
- Model parameters
- Feature window sizes

## Testing

```bash
pytest tests/ -v
```

## Production Deployment

- Deploy on AWS EMR (Spark) + EC2 (Kafka) or Databricks
- Use Confluent Cloud for managed Kafka
- Configure Cassandra replication factor = 3
- Enable TLS encryption for all connections
- Set up CloudWatch/DataDog monitoring

## Contributing

1. Fork & create feature branch
2. Write tests for new features
3. Run pytest to ensure coverage >80%
4. Create PR with description

## License

MIT
