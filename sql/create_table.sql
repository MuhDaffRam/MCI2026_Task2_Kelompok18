CREATE TABLE IF NOT EXISTS analytics.orders_top_products (
    product_name String,
    department String,
    total_orders Int32,
    reorder_count Int32,
    unique_users Int32
)
ENGINE = MergeTree()
ORDER BY total_orders;