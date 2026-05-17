#!/usr/bin/env python3


import clickhouse_connect


CH_HOST = 'lab2_clickhouse'
CH_PORT = 8123
CH_DB = 'lab2_reports'
CH_USER = 'default'
CH_PASSWORD = 'mypassword123'  


client = clickhouse_connect.get_client(
    host=CH_HOST,
    port=CH_PORT,
    database=CH_DB,
    username=CH_USER,
    password=CH_PASSWORD
)
print(" Подключено к ClickHouse")


VITRINES = {
    # ==================== ПРОДУКТЫ ====================
    
    "rep_products_top10": """
        CREATE TABLE IF NOT EXISTS rep_products_top10
        ENGINE = MergeTree()
        ORDER BY rank_by_sales
        AS 
        SELECT 
            product_id, product_name, category, brand,
            total_revenue, total_quantity, avg_rating,
            total_reviews, order_count, rank_by_sales
        FROM rep_products 
        WHERE rank_by_sales <= 10
        ORDER BY rank_by_sales
        LIMIT 10
    """,
    
    "rep_products_by_category": """
        CREATE TABLE IF NOT EXISTS rep_products_by_category
        ENGINE = MergeTree()
        ORDER BY category_revenue DESC
        AS 
        SELECT 
            category,
            count(DISTINCT product_id) as product_count,
            round(sum(total_revenue), 2) as category_revenue,
            sum(total_quantity) as category_quantity,
            round(avg(avg_rating), 2) as avg_category_rating,
            sum(total_reviews) as total_reviews
        FROM rep_products
        GROUP BY category
        ORDER BY category_revenue DESC
    """,
    
    "rep_products_ratings": """
        CREATE TABLE IF NOT EXISTS rep_products_ratings
        ENGINE = MergeTree()
        ORDER BY avg_rating DESC
        AS 
        SELECT 
            product_name, category,
            round(avg_rating, 2) as avg_rating,
            total_reviews, total_quantity,
            round(avg_rating * total_quantity, 2) as quality_score
        FROM rep_products
        WHERE avg_rating IS NOT NULL
        ORDER BY avg_rating DESC
        LIMIT 100
    """,
    
    # ==================== КЛИЕНТЫ ====================
    
    "rep_customers_top10": """
        CREATE TABLE IF NOT EXISTS rep_customers_top10
        ENGINE = MergeTree()
        ORDER BY rank_by_spend
        AS 
        SELECT 
            customer_id,
            concat(customer_first_name, ' ', customer_last_name) as full_name,
            customer_country, customer_email,
            round(total_spent, 2) as total_spent,
            order_count, round(avg_check, 2) as avg_check,
            total_items, rank_by_spend
        FROM rep_customers 
        WHERE rank_by_spend <= 10
        ORDER BY rank_by_spend
        LIMIT 10
    """,
    
    "rep_customers_by_country": """
        CREATE TABLE IF NOT EXISTS rep_customers_by_country
        ENGINE = MergeTree()
        ORDER BY country_revenue DESC
        AS 
        SELECT 
            customer_country,
            count(DISTINCT customer_id) as customer_count,
            round(sum(total_spent), 2) as country_revenue,
            round(avg(avg_check), 2) as avg_check_country,
            sum(order_count) as total_orders
        FROM rep_customers
        GROUP BY customer_country
        ORDER BY country_revenue DESC
    """,
    
    "rep_customers_segments": """
        CREATE TABLE IF NOT EXISTS rep_customers_segments
        ENGINE = MergeTree()
        ORDER BY segment_order
        AS 
        SELECT 
            CASE 
                WHEN avg_check >= 500 THEN 'Premium'
                WHEN avg_check >= 200 THEN 'Standard'
                WHEN avg_check >= 50 THEN 'Budget'
                ELSE 'Low'
            END as segment,
            CASE 
                WHEN avg_check >= 500 THEN 1
                WHEN avg_check >= 200 THEN 2
                WHEN avg_check >= 50 THEN 3
                ELSE 4
            END as segment_order,
            count(DISTINCT customer_id) as customer_count,
            round(avg(total_spent), 2) as avg_total_spent,
            round(avg(avg_check), 2) as avg_check_segment
        FROM rep_customers
        GROUP BY segment, segment_order
        ORDER BY segment_order
    """,
    
    # ==================== ВРЕМЯ ====================
    
    "rep_time_monthly": """
        CREATE TABLE IF NOT EXISTS rep_time_monthly
        ENGINE = MergeTree()
        ORDER BY (year, month)
        AS 
        SELECT 
            year, month, total_revenue, total_orders,
            round(avg_order_size, 2) as avg_order_size, total_units,
            lag(total_revenue) OVER (ORDER BY year, month) as prev_month_revenue,
            if(prev_month_revenue > 0 AND prev_month_revenue != total_revenue,
               round((total_revenue - prev_month_revenue) / prev_month_revenue * 100, 2),
               NULL) as revenue_growth_pct
        FROM rep_time
        ORDER BY year, month
    """,
    
    "rep_time_yearly": """
        CREATE TABLE IF NOT EXISTS rep_time_yearly
        ENGINE = MergeTree()
        ORDER BY year
        AS 
        SELECT 
            year,
            round(sum(total_revenue), 2) as yearly_revenue,
            sum(total_orders) as yearly_orders,
            round(avg(avg_order_size), 2) as avg_order_size_yearly,
            sum(total_units) as yearly_units
        FROM rep_time
        GROUP BY year
        ORDER BY year
    """,
    
    "rep_time_comparison": """
        CREATE TABLE IF NOT EXISTS rep_time_comparison
        ENGINE = MergeTree()
        ORDER BY period
        AS 
        SELECT 
            concat(toString(year), '-', LPAD(toString(month), 2, '0')) as period,
            total_revenue, total_orders,
            round(avg_order_size, 2) as avg_order_size
        FROM rep_time
        ORDER BY year, month
        LIMIT 24
    """,
    
    # ==================== МАГАЗИНЫ ====================
    
    "rep_stores_top5": """
        CREATE TABLE IF NOT EXISTS rep_stores_top5
        ENGINE = MergeTree()
        ORDER BY rank_by_revenue
        AS 
        SELECT 
            store_id, store_name, store_city, store_state, store_country,
            round(total_revenue, 2) as total_revenue,
            total_orders, round(avg_check, 2) as avg_check,
            total_units, rank_by_revenue
        FROM rep_stores 
        WHERE rank_by_revenue <= 5
        ORDER BY rank_by_revenue
        LIMIT 5
    """,
    
    "rep_stores_by_location": """
        CREATE TABLE IF NOT EXISTS rep_stores_by_location
        ENGINE = MergeTree()
        ORDER BY (store_country, city_revenue DESC)
        AS 
        SELECT 
            store_country, store_city,
            count(DISTINCT store_id) as store_count,
            round(sum(total_revenue), 2) as city_revenue,
            round(avg(avg_check), 2) as avg_check_city,
            sum(total_orders) as total_orders_city
        FROM rep_stores
        GROUP BY store_country, store_city
        ORDER BY store_country, city_revenue DESC
    """,
    
    "rep_stores_metrics": """
        CREATE TABLE IF NOT EXISTS rep_stores_metrics
        ENGINE = MergeTree()
        ORDER BY total_revenue DESC
        AS 
        SELECT 
            store_name, store_city,
            round(total_revenue, 2) as total_revenue,
            total_orders, round(avg_check, 2) as avg_check,
            round(total_revenue / nullIf(total_orders, 0), 2) as revenue_per_order
        FROM rep_stores
        ORDER BY total_revenue DESC
    """,
    
    # ==================== ПОСТАВЩИКИ ====================
    
    "rep_suppliers_top5": """
        CREATE TABLE IF NOT EXISTS rep_suppliers_top5
        ENGINE = MergeTree()
        ORDER BY rank_by_revenue
        AS 
        SELECT 
            supplier_id, supplier_name, supplier_country, supplier_city,
            round(total_revenue, 2) as total_revenue,
            total_qty, round(avg_product_price, 2) as avg_product_price,
            product_count, rank_by_revenue
        FROM rep_suppliers 
        WHERE rank_by_revenue <= 5
        ORDER BY rank_by_revenue
        LIMIT 5
    """,
    
    "rep_suppliers_by_country": """
        CREATE TABLE IF NOT EXISTS rep_suppliers_by_country
        ENGINE = MergeTree()
        ORDER BY country_revenue DESC
        AS 
        SELECT 
            supplier_country,
            count(DISTINCT supplier_id) as supplier_count,
            round(sum(total_revenue), 2) as country_revenue,
            round(avg(avg_product_price), 2) as avg_price_country,
            sum(total_qty) as total_qty_supplied
        FROM rep_suppliers
        GROUP BY supplier_country
        ORDER BY country_revenue DESC
    """,
    
    "rep_suppliers_pricing": """
        CREATE TABLE IF NOT EXISTS rep_suppliers_pricing
        ENGINE = MergeTree()
        ORDER BY total_revenue DESC
        AS 
        SELECT 
            supplier_name, supplier_country,
            round(avg_product_price, 2) as avg_product_price,
            product_count,
            round(total_revenue, 2) as total_revenue,
            round(total_revenue / nullIf(product_count, 0), 2) as revenue_per_product
        FROM rep_suppliers
        ORDER BY total_revenue DESC
    """,
    
    # ==================== КАЧЕСТВО ====================
    
    "rep_quality_extremes": """
        CREATE TABLE IF NOT EXISTS rep_quality_extremes
        ENGINE = MergeTree()
        ORDER BY (rating_type, avg_rating DESC)
        AS 
        SELECT 
            product_name, category,
            round(avg_rating, 2) as avg_rating,
            total_reviews, sales_volume,
            'Highest' as rating_type
        FROM rep_quality
        WHERE avg_rating >= 4.5
        UNION ALL
        SELECT 
            product_name, category,
            round(avg_rating, 2) as avg_rating,
            total_reviews, sales_volume,
            'Lowest' as rating_type
        FROM rep_quality
        WHERE avg_rating <= 2.5
        ORDER BY rating_type, avg_rating DESC
    """,
    
    "rep_quality_correlation": """
        CREATE TABLE IF NOT EXISTS rep_quality_correlation
        ENGINE = MergeTree()
        ORDER BY rating_order
        AS 
        SELECT 
            CASE 
                WHEN avg_rating >= 4.5 THEN '⭐⭐⭐⭐⭐ Excellent (4.5+)'
                WHEN avg_rating >= 4.0 THEN '⭐⭐⭐⭐ Good (4.0-4.49)'
                WHEN avg_rating >= 3.0 THEN '⭐⭐⭐ Average (3.0-3.99)'
                ELSE '⭐⭐ Below Average (<3.0)'
            END as rating_category,
            CASE 
                WHEN avg_rating >= 4.5 THEN 1
                WHEN avg_rating >= 4.0 THEN 2
                WHEN avg_rating >= 3.0 THEN 3
                ELSE 4
            END as rating_order,
            count(DISTINCT product_id) as product_count,
            round(avg(sales_volume), 0) as avg_sales_by_rating,
            round(avg(revenue), 2) as avg_revenue_by_rating,
            round(avg(avg_rating), 2) as avg_rating_in_group
        FROM rep_quality
        WHERE avg_rating IS NOT NULL
        GROUP BY rating_category, rating_order
        ORDER BY rating_order
    """,
    
    "rep_quality_most_reviewed": """
        CREATE TABLE IF NOT EXISTS rep_quality_most_reviewed
        ENGINE = MergeTree()
        ORDER BY total_reviews DESC
        AS 
        SELECT 
            product_name, category,
            round(avg_rating, 2) as avg_rating,
            total_reviews, sales_volume,
            round(revenue, 2) as revenue,
            round(total_reviews * avg_rating, 0) as hype_index
        FROM rep_quality
        WHERE total_reviews > 0
        ORDER BY total_reviews DESC
        LIMIT 50
    """
}
# === Создание витрин ===
print(" Создаю аналитические витрины...")
for name, query in VITRINES.items():
    try:
        client.command(query)
        print(f"{name}: создана")
    except Exception as e:
        print(f" {name}: ошибка — {e}")

# === Проверка: количество строк в каждой витрине ===
print("\n Статистика по витринам:")
for name in VITRINES.keys():
    try:
        count = client.query(f"SELECT count() FROM {name}").first_row[0]
        print(f"  • {name}: {count:,} строк")
    except:
        print(f"  • {name}: (не создана)")

client.close()
