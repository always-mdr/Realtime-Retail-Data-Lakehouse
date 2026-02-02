import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, TimestampType

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# 1. Initialize Spark Session
# ---------------------------------------------------------
spark = SparkSession.builder \
    .appName("RetailStreamProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "hadoop") \
    .config("spark.sql.catalog.demo.warehouse", "s3a://retail-lake/iceberg_warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio_admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio_password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

logger.info("Spark Session Started Successfully")

# ---------------------------------------------------------
# 1.5 Setup Iceberg Table (Run once)
# ---------------------------------------------------------
logger.info("Checking/Creating Iceberg Table...")
spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.retail")
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.retail.orders (
        order_id STRING,
        customer_id STRING,
        product_id STRING,
        price DOUBLE,
        quantity INT,
        timestamp TIMESTAMP
    ) USING iceberg
""")

# ---------------------------------------------------------
# 2. Define Schema & Read Stream
# ---------------------------------------------------------
schema = StructType() \
    .add("order_id", StringType()) \
    .add("customer_id", StringType()) \
    .add("product_id", StringType()) \
    .add("price", DoubleType()) \
    .add("quantity", IntegerType()) \
    .add("timestamp", TimestampType())

# Read from Redpanda
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:29092") \
    .option("subscribe", "retail_orders") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON
parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# ---------------------------------------------------------
# 3. Write Stream to Iceberg
# ---------------------------------------------------------
query = parsed_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .option("path", "demo.retail.orders") \
    .option("checkpointLocation", "s3a://retail-lake/checkpoints/orders_iceberg") \
    .start()

logger.info("Streaming to Iceberg...")
query.awaitTermination()