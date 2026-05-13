

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, count, desc, rank, round as spark_round,
    date_format, to_date, lit
)
from pyspark.sql.window import Window


PG_URL = "jdbc:postgresql://lab2_postgres:5432/lab2_db"


CH_URL = "jdbc:clickhouse://lab2_clickhouse:8123/lab2_reports"

PG_PROPS = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

CH_PROPS = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "default",
    "password": ""
}


spark = SparkSession.builder \
    .appName("Lab2_Reports_ClickHouse") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.5.0") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

print(" Spark session created")

def load_pg(table):
    return spark.read.jdbc(url=PG_URL, table=table, properties=PG_PROPS)

fact = load_pg("fact_sales")
cust = load_pg("dim_customer")
prod = load_pg("dim_product")
store = load_pg("dim_store")
supp = load_pg("dim_supplier")
dt = load_pg("dim_date")

df = fact \
    .join(cust, "customer_id", "left") \
    .join(prod, "product_id", "left") \
    .join(store, ["store_name", "store_city", "store_country"], "left") \
    .join(supp, "supplier_name", "left") \
    .join(dt, "date_id", "left")

print(f" Объединено строк: {df.count()}")

rep_products = df.groupBy("product_id", "product_name", "category", "brand") \
    .agg(
        _sum("total_amount").alias("total_revenue"),
        _sum("quantity").alias("total_quantity"),
        avg("product_rating").alias("avg_rating"),
        _sum("product_reviews").alias("total_reviews"),
        count("*").alias("order_count")
    ) \
    .withColumn("rank_by_sales", rank().over(Window.orderBy(desc("total_quantity")))) \
    .orderBy(desc("total_revenue"))

rep_customers = df.groupBy("customer_id", "customer_first_name", "customer_last_name", "customer_country", "customer_email") \
    .agg(
        _sum("total_amount").alias("total_spent"),
        count("*").alias("order_count"),
        spark_round(avg("total_amount"), 2).alias("avg_check"),
        _sum("quantity").alias("total_items")
    ) \
    .withColumn("rank_by_spend", rank().over(Window.orderBy(desc("total_spent")))) \
    .orderBy(desc("total_spent"))


rep_time = df.groupBy("year", "month") \
    .agg(
        _sum("total_amount").alias("total_revenue"),
        count("*").alias("total_orders"),
        spark_round(avg("total_amount"), 2).alias("avg_order_size"),
        _sum("quantity").alias("total_units")
    ) \
    .orderBy("year", "month")

rep_stores = df.groupBy("store_id", "store_name", "store_city", "store_state", "store_country") \
    .agg(
        _sum("total_amount").alias("total_revenue"),
        count("*").alias("total_orders"),
        spark_round(avg("total_amount"), 2).alias("avg_check"),
        _sum("quantity").alias("total_units")
    ) \
    .withColumn("rank_by_revenue", rank().over(Window.orderBy(desc("total_revenue")))) \
    .orderBy(desc("total_revenue"))

rep_suppliers = df.groupBy("supplier_id", "supplier_name", "supplier_country", "supplier_city") \
    .agg(
        _sum("total_amount").alias("total_revenue"),
        _sum("quantity").alias("total_qty"),
        spark_round(avg("unit_price"), 2).alias("avg_product_price"),
        count("product_id").alias("product_count")
    ) \
    .withColumn("rank_by_revenue", rank().over(Window.orderBy(desc("total_revenue")))) \
    .orderBy(desc("total_revenue"))


rep_quality = df.groupBy("product_id", "product_name", "category") \
    .agg(
        avg("product_rating").alias("avg_rating"),
        _sum("product_reviews").alias("total_reviews"),
        _sum("quantity").alias("sales_volume"),
        _sum("total_amount").alias("revenue"),
        spark_round(avg("product_rating") * _sum("quantity"), 2).alias("quality_score")
    ) \
    .orderBy(desc("sales_volume"))


reports = {
    "rep_products": rep_products,
    "rep_customers": rep_customers,
    "rep_time": rep_time,
    "rep_stores": rep_stores,
    "rep_suppliers": rep_suppliers,
    "rep_quality": rep_quality
}

import clickhouse_connect

client = clickhouse_connect.get_client(
    host='lab2_clickhouse',
    port=8123,
    database='lab2_reports',
    username='default',
    password='mypassword123' 
)

def create_ch_table(client, table_name, schema):

    columns = ', '.join([f"{col} {dtype}" for col, dtype in schema.items()])
    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns}
    ) ENGINE = MergeTree()
    ORDER BY tuple()
    """
    
    client.command(query)
    print(f" Таблица {table_name} создана/проверена")

SCHEMAS = {
    "rep_products": {
        "product_id": "UInt32",
        "product_name": "Nullable(String)",
        "category": "Nullable(String)",
        "brand": "Nullable(String)",
        "total_revenue": "Float64",
        "total_quantity": "UInt64",
        "avg_rating": "Nullable(Float32)",
        "total_reviews": "Nullable(UInt32)",
        "order_count": "UInt64",
        "rank_by_sales": "UInt32"
    },
    "rep_customers": {
        "customer_id": "UInt32",
        "customer_first_name": "Nullable(String)",
        "customer_last_name": "Nullable(String)",
        "customer_country": "Nullable(String)",
        "customer_email": "Nullable(String)",
        "total_spent": "Float64",
        "order_count": "UInt64",
        "avg_check": "Nullable(Float32)",
        "total_items": "UInt64",
        "rank_by_spend": "UInt32"
    },
    "rep_time": {
        "year": "UInt16",
        "month": "UInt8",
        "total_revenue": "Float64",
        "total_orders": "UInt64",
        "avg_order_size": "Nullable(Float32)",
        "total_units": "UInt64"
    },
    "rep_stores": {
        "store_id": "UInt32",
        "store_name": "Nullable(String)",
        "store_city": "Nullable(String)",
        "store_state": "Nullable(String)",   
        "store_country": "Nullable(String)",
        "total_revenue": "Float64",
        "total_orders": "UInt64",
        "avg_check": "Nullable(Float32)",
        "total_units": "UInt64",
        "rank_by_revenue": "UInt32"
    },
    "rep_suppliers": {
        "supplier_id": "UInt32",
        "supplier_name": "Nullable(String)",
        "supplier_country": "Nullable(String)",
        "supplier_city": "Nullable(String)",
        "total_revenue": "Float64",
        "total_qty": "UInt64",
        "avg_product_price": "Nullable(Float32)",
        "product_count": "UInt64",
        "rank_by_revenue": "UInt32"
    },
    "rep_quality": {
        "product_id": "UInt32",
        "product_name": "Nullable(String)",
        "category": "Nullable(String)",
        "avg_rating": "Nullable(Float32)",
        "total_reviews": "Nullable(UInt32)",
        "sales_volume": "UInt64",
        "revenue": "Float64",
        "quality_score": "Nullable(Float64)"
    }
}

reports = {
    "rep_products": rep_products,
    "rep_customers": rep_customers,
    "rep_time": rep_time,
    "rep_stores": rep_stores,
    "rep_suppliers": rep_suppliers,
    "rep_quality": rep_quality
}

for table_name, df_report in reports.items():
   
    create_ch_table(client, table_name, SCHEMAS[table_name])
    
    pdf = df_report.toPandas()
    client.insert_df(table_name, pdf)
    print(f" {table_name}: {len(pdf)} строк записано в ClickHouse")

client.close()
for table_name, df_report in reports.items():

    pdf = df_report.toPandas()
    client.insert_df(table_name, pdf)
    print(f"{table_name}: {len(pdf)} строк записано в ClickHouse")
    
print("\n Все отчёты сгенерированы!")
spark.stop()