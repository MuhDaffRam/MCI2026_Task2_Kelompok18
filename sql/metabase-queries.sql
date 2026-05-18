-- =============================
-- 1. Tabel
-- =============================
-- a. Agregasi Dasar (Analytics)
SELECT
    SUM(total_orders) AS total_order_lines,
    SUM(reorder_count) AS total_reorders,
    SUM(unique_users) AS total_unique_users,
    AVG(total_orders) AS avg_orders_per_product,
    AVG(reorder_count) AS avg_reorders_per_product,
    AVG(unique_users) AS avg_users_per_product
FROM analytics.orders_top_products;

-- =============================
-- 2. Diagram Batang
-- =============================
-- a. Top 100 Produk (Analytics)
SELECT
    product_name,
    total_orders
FROM analytics.orders_top_products
ORDER BY total_orders DESC
LIMIT 100;