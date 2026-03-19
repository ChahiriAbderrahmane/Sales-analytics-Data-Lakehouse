from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Export Aggregates to S3") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.access.key", "YourKey") \
    .config("spark.hadoop.fs.s3a.secret.key", "YourSecretKey") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

print("Reading delta table from hdfs...")
df = spark.read.format("delta").load("hdfs:///user/hadoop/sales_data_mart/gold/agg_monthly_sales")

print(f"Rows number found : {df.count()}")
df.printSchema()

print("Export vers S3...")
df.write \
    .mode("overwrite") \
    .parquet("s3a://sales-data-mart-internet-sales-process/gold-layer-aggregate-tables/agg_monthly_sales/")

print("Exporting completed successfully.")
spark.stop()