-- ## TAB ANALYTICS ##
-- =============================
-- 1. Tabel
-- =============================
-- a. KPI
SELECT
    SUM(total_orders) AS total_order_lines,
    SUM(reorder_count) AS total_reorders,
    SUM(unique_users) AS total_unique_users,
    AVG(total_orders) AS avg_orders_per_product,
    AVG(reorder_count) AS avg_reorders_per_product,
    AVG(unique_users) AS avg_users_per_product
FROM analytics.orders_top_products;

-- =============================
-- 2. Diagram Baris
-- =============================
-- a. Top 10 Products
SELECT
    product_name,
    total_orders
FROM analytics.orders_top_products
ORDER BY total_orders DESC
LIMIT 10;

-- b. Top 10 Reordered Products
SELECT
    product_name,
    reorder_count
FROM analytics.orders_top_products
ORDER BY reorder_count DESC
LIMIT 10;

-- =============================
-- 3. Pie Chart
-- =============================
-- a. Departement Contribution
SELECT
    department,
    SUM(total_orders) AS total_orders
FROM analytics.orders_top_products
GROUP BY department
ORDER BY total_orders DESC;

-- =============================
-- 3. Diagram Batang
-- =============================
-- a. Top 10 Products dengan User Terbanyak
SELECT
    product_name,
    unique_users
FROM analytics.orders_top_products
ORDER BY unique_users DESC
LIMIT 10;

-- ==========================================================================================

-- ## TAB RAW ##
-- =============================
-- 1. Angka
-- =============================
-- a. Average add-to-cart order
SELECT
    AVG(add_to_cart_order) AS avg_cart_position
FROM orders_db.orders;

-- =============================
-- 2. Diagram Garis - Batang
-- =============================
-- a. Transaction per hour
SELECT
    order_hour_of_day,
    COUNT(*) AS total_orders
FROM orders_db.orders
GROUP BY order_hour_of_day
ORDER BY order_hour_of_day;

-- =============================
-- 3. Pie Chart
-- =============================
-- a. Reorder vs First Purchase
SELECT
    CASE
        WHEN reordered = 1 THEN 'Reordered Product'
        ELSE 'First Purchase'
    END AS reorder_status,
    COUNT(*) AS total
FROM orders_db.orders
GROUP BY reorder_status;

-- =============================
-- 4. Waterfall
-- =============================
-- a. Top 10 Aisle
SELECT
    aisle,
    COUNT(*) AS total
FROM orders_db.orders
GROUP BY aisle
ORDER BY total DESC
LIMIT 10;