from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StringType, IntegerType, BooleanType

# 1. Initialize Spark with Delta Lake & S3 dependencies
spark = SparkSession.builder \
    .appName("WikiLakehouse") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://my-minio.default.svc.cluster.local:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 2. Define Schema (simplified for brevity)
schema = StructType() \
    .add("user", StringType()) \
    .add("title", StringType()) \
    .add("bot", BooleanType()) \
    .add("timestamp", IntegerType()) \
    .add("server_name", StringType())

# 3. Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "my-kafka.default.svc.cluster.local:9092") \
    .option("subscribe", "wiki-edits") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Transform Data
parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .filter(col("bot") == False)  # Filter out bots (business logic)

# 5. Write to MinIO (Delta Lake)
query = parsed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/wiki_edits") \
    .option("path", "s3a://lakehouse/delta/wiki_edits") \
    .start()

query.awaitTermination()