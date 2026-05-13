#!/usr/bin/env python3


from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, trim, row_number, year, month, dayofweek, 
    when, lit, coalesce
)
from pyspark.sql.window import Window
import os


DATA_PATH = "/opt/spark/data"
PG_URL = "jdbc:postgresql://lab2_postgres:5432/lab2_db"
PG_PROPS = {
    "user": "postgres",
    "password": "postgres", 
    "driver": "org.postgresql.Driver"
}


spark = SparkSession.builder \
    .appName("Lab2_ETL_PostgreSQL") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

print("📡 Spark session created")


files = [
    "MOCK_DATA.csv", "MOCK_DATA (1).csv", "MOCK_DATA (2).csv",
    "MOCK_DATA (3).csv", "MOCK_DATA (4).csv", "MOCK_DATA (5).csv",
    "MOCK_DATA (6).csv", "MOCK_DATA (7).csv", "MOCK_DATA (8).csv",
    "MOCK_DATA (9).csv"
]

df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", "\"") \
    .option("quote", "\"") \
    .csv([f"{DATA_PATH}/{f}" for f in files])

print(f"Прочитано строк: {df_raw.count()}")

df = df_raw \
    .withColumn("sale_date", to_date(trim(col("sale_date")), "M/d/yyyy")) \
    .withColumn("product_release_date", to_date(trim(col("product_release_date")), "M/d/yyyy")) \
    .withColumn("product_expiry_date", to_date(trim(col("product_expiry_date")), "M/d/yyyy")) \
    .withColumn("product_price", col("product_price").cast("decimal(10,2)")) \
    .withColumn("sale_total_price", col("sale_total_price").cast("decimal(12,2)")) \
    .dropna(subset=["sale_date", "sale_customer_id", "sale_product_id"])


dim_customer = df.select(
    col("sale_customer_id").alias("customer_id").cast("int"),
    col("customer_first_name"),
    col("customer_last_name"),
    col("customer_age").cast("int"),
    col("customer_email"),
    col("customer_country"),
    col("customer_pet_type"),
    col("customer_pet_name"),
    col("customer_pet_breed")
).dropDuplicates(["customer_id"])


dim_product = df.select(
    col("sale_product_id").alias("product_id").cast("int"),
    col("product_name"),
    col("product_category").alias("category"),
    col("product_brand").alias("brand"),
    col("product_weight").cast("decimal(10,2)"),
    col("product_color"),
    col("product_size"),
    col("product_material"),
    col("product_description"),
    col("product_rating").cast("decimal(3,2)"),
    col("product_reviews").cast("int"),
    col("product_release_date"),
    col("product_expiry_date")
).dropDuplicates(["product_id"])


window_store = Window.orderBy("store_name", "store_city", "store_country")
dim_store = df.select(
    row_number().over(window_store).alias("store_id").cast("int"),
    col("store_name"),
    col("store_location"),
    col("store_city"),
    col("store_state"),
    col("store_country"),
    col("store_phone"),
    col("store_email")
).dropDuplicates(["store_name", "store_city", "store_country"])


window_supplier = Window.orderBy("supplier_name")
dim_supplier = df.select(
    row_number().over(window_supplier).alias("supplier_id").cast("int"),
    col("supplier_name"),
    col("supplier_contact"),
    col("supplier_email"),
    col("supplier_phone"),
    col("supplier_address"),
    col("supplier_city"),
    col("supplier_country")
).dropDuplicates(["supplier_name", "supplier_contact"])


dim_date = df.select(col("sale_date").alias("date_id")).dropDuplicates() \
    .withColumn("year", year("date_id")) \
    .withColumn("month", month("date_id")) \
    .withColumn("day_of_week", dayofweek("date_id")) \
    .withColumn("quarter", ((month("date_id") + 2) / 3).cast("int"))


fact_sales = df.select(
    col("sale_date").alias("date_id"),
    col("sale_customer_id").alias("customer_id").cast("int"),
    col("sale_product_id").alias("product_id").cast("int"),
    col("store_name"),
    col("store_city"),
    col("store_country"),
    col("supplier_name"),
    col("sale_quantity").alias("quantity").cast("int"),
    col("product_price").alias("unit_price"),
    col("sale_total_price").alias("total_amount"),
    col("id").alias("source_id").cast("int")
)


tables = {
    "dim_customer": dim_customer,
    "dim_product": dim_product,
    "dim_store": dim_store,
    "dim_supplier": dim_supplier,
    "dim_date": dim_date,
    "fact_sales": fact_sales
}

for table_name, df_table in tables.items():
    df_table.write \
        .jdbc(url=PG_URL, table=table_name, mode="overwrite", properties=PG_PROPS)
    print(f"{table_name}: {df_table.count()} строк записано")

print("\n ETL завершён!")
spark.stop()