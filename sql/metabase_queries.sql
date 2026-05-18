-- Top Product
SELECT
    product_name,
    COUNT(*) AS total_orders
FROM orders_db.orders
GROUP BY product_name
ORDER BY total_orders DESC
LIMIT 10;

-- Top Department
SELECT
    department,
    COUNT(*) AS total
FROM orders_db.orders
GROUP BY department
ORDER BY total DESC;

-- Reordered Products
SELECT
    reordered,
    COUNT(*) AS total
FROM orders_db.orders
GROUP BY reordered;

-- Orders by Hour
SELECT
    order_hour_of_day,
    COUNT(*) AS total_orders
FROM orders_db.orders
GROUP BY order_hour_of_day
ORDER BY order_hour_of_day;