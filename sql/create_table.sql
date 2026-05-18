CREATE TABLE IF NOT EXISTS orders_db.orders (
    order_id UInt32,
    user_id UInt32,
    order_number UInt32,
    order_dow UInt8,
    order_hour_of_day UInt8,
    days_since_prior_order Float32,
    eval_set String,

    product_id UInt32,
    product_name String,

    aisle_id UInt32,
    aisle String,

    department_id UInt32,
    department String,

    add_to_cart_order UInt32,
    reordered UInt8
)
ENGINE = MergeTree()
ORDER BY (order_id, product_id);