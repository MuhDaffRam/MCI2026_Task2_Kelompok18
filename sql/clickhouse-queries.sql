# Jumlah produk distinct di snapshot terbaru
SELECT COUNT(*) FROM orders_top_products;


# ===== Lihat data dulu =====

# Lima produk paling laris
SELECT product_name, department, total_orders, reorder_count, unique_users
FROM orders_top_products
ORDER BY total_orders DESC
LIMIT 5;

# Sepuluh produk dengan reorder paling banyak
SELECT product_name, reorder_count
FROM orders_top_products
ORDER BY reorder_count DESC
LIMIT 10;

# Sepuluh produk dengan unique users terbanyak
SELECT product_name, unique_users
FROM orders_top_products
ORDER BY unique_users DESC
LIMIT 10;

# Sepuluh produk dengan reorder paling sedikit
SELECT product_name, reorder_count, total_orders
FROM orders_top_products
ORDER BY reorder_count ASC
LIMIT 10;


# ===== Aggregate dasar =====

# Total seluruh order line yang tercatat
SELECT SUM(total_orders) FROM orders_top_products;

# Total reorder secara global
SELECT SUM(reorder_count) FROM orders_top_products;

# Total unique user yang ter-cover
SELECT SUM(unique_users) FROM orders_top_products;

# Rata-rata order per produk
SELECT AVG(total_orders) FROM orders_top_products;

# Rata-rata reorder per produk
SELECT AVG(reorder_count) FROM orders_top_products;

# Rata-rata unique users per produk
SELECT AVG(unique_users) FROM orders_top_products;


# ===== Operasi sederhana di kolom =====

# Selisih total_orders dan reorder = order pertama kali (bukan reorder)
SELECT product_name, total_orders, reorder_count, total_orders - reorder_count
FROM orders_top_products
ORDER BY total_orders DESC;

# Rasio reorder per total order (kasar — tanpa filter)
SELECT product_name, reorder_count, total_orders, reorder_count * 100.0 / total_orders
FROM orders_top_products
ORDER BY total_orders DESC
LIMIT 10;


# ===== Group by sederhana =====

# Berapa produk top yang masuk per department
SELECT department, COUNT(*)
FROM orders_top_products
GROUP BY department
ORDER BY COUNT(*) DESC;

# Total orders per department (gabungan top products-nya)
SELECT department, SUM(total_orders)
FROM orders_top_products
GROUP BY department
ORDER BY SUM(total_orders) DESC;