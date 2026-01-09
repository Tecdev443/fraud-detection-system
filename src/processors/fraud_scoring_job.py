from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, avg, max, min, stddev,
    when, coalesce, rank, lag, unix_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegressionModel
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName('fraud-scoring-engine') \
    .config('spark.sql.streaming.schemaInference', 'true') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# Schema for transaction events
TRANSACTION_SCHEMA = StructType([
    StructField('transaction_id', StringType()),
    StructField('user_id', StringType()),
    StructField('amount', DoubleType()),
    StructField('currency', StringType()),
    StructField('merchant', StringType()),
    StructField('card_country', StringType()),
    StructField('card_type', StringType()),
    StructField('device_type', StringType()),
    StructField('timestamp', StringType()),
    StructField('user_age', LongType()),
    StructField('account_age_days', LongType()),
    StructField('previous_txn_count', LongType()),
    StructField('is_fraud_label', BooleanType())
])

def create_features(df):
    """Compute fraud detection features"""
    
    # Windowed aggregations (10-minute windows)
    window_spec = window('timestamp_parsed', '10 minutes')
    
    windowed_stats = df.withColumn('timestamp_parsed', col('timestamp').cast('timestamp')) \
        .groupBy('user_id', window_spec) \
        .agg(
            count('*').alias('txn_count_10m'),
            avg('amount').alias('avg_amount_10m'),
            max('amount').alias('max_amount_10m'),
            stddev('amount').alias('stddev_amount_10m')
        )
    
    # Join back to original
    df_with_features = df.join(
        windowed_stats,
        (df.user_id == windowed_stats.user_id),
        'left'
    )
    
    # Compute derived features
    features = df_with_features.select(
        '*',
        # Risk indicators
        when(col('amount') > col('max_amount_10m') * 2, 1).otherwise(0).alias('high_amount_flag'),
        when(col('card_country') != 'US', 1).otherwise(0).alias('foreign_country_flag'),
        when(col('device_type') == 'atm', 1).otherwise(0).alias('atm_flag'),
        when(col('card_type') == 'AMEX', 1).otherwise(0).alias('amex_flag'),
        (col('txn_count_10m') / 10.0).alias('velocity_per_min'),
        coalesce(col('stddev_amount_10m'), 0).alias('amount_variance'),
        col('account_age_days').alias('account_age'),
        col('previous_txn_count').alias('historical_count')
    )
    
    return features

def compute_fraud_score(df):
    """Simple rule-based fraud scoring (can replace with ML model)"""
    
    scored = df.withColumn('fraud_score',
        when(col('amount') > 1000, 0.7).otherwise(0.1) +
        when(col('foreign_country_flag') == 1, 0.3).otherwise(0) +
        when(col('high_amount_flag') == 1, 0.2).otherwise(0) +
        when(col('velocity_per_min') > 0.5, 0.15).otherwise(0)
    ).withColumn(
        'fraud_prediction',
        when(col('fraud_score') > 0.6, 1).otherwise(0)
    ).withColumn(
        'risk_level',
        when(col('fraud_score') > 0.8, 'CRITICAL')
        .when(col('fraud_score') > 0.6, 'HIGH')
        .when(col('fraud_score') > 0.3, 'MEDIUM')
        .otherwise('LOW')
    )
    
    return scored

if __name__ == '__main__':
    logger.info('Starting Fraud Scoring Engine')
    
    # Read from Kafka
    df_kafka = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'localhost:9092') \
        .option('subscribe', 'transactions') \
        .option('startingOffsets', 'latest') \
        .load()
    
    # Parse JSON
    df_parsed = df_kafka.select(
        from_json(col('value').cast('string'), TRANSACTION_SCHEMA).alias('data')
    ).select('data.*')
    
    # Feature engineering
    df_features = create_features(df_parsed)
    
    # Fraud scoring
    df_scored = compute_fraud_score(df_features)
    
    # Console output for testing
    query = df_scored.select(
        'transaction_id', 'user_id', 'amount', 'fraud_score', 'fraud_prediction', 'risk_level'
    ).writeStream \
        .format('console') \
        .option('truncate', False) \
        .option('checkpointLocation', '/tmp/fraud_checkpoint') \
        .start()
    
    query.awaitTermination()
