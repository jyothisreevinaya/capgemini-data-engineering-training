customers.createOrReplaceTempView("customers")
orders.createOrReplaceTempView("orders")
order_items.createOrReplaceTempView("order_items")
products.createOrReplaceTempView("products")

#  Task 1: Top 3 Customers per City
SELECT *
FROM (
    SELECT 
        c.customer_city,
        c.customer_id,
        SUM(oi.price) AS total_spend,
        RANK() OVER (PARTITION BY c.customer_city ORDER BY SUM(oi.price) DESC) AS rank
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    JOIN customers c ON o.customer_id = c.customer_id
    GROUP BY c.customer_city, c.customer_id
) t
WHERE rank <= 3;

# Task 2: Running Total of Sales
SELECT 
    date,
    daily_sales,
    SUM(daily_sales) OVER (ORDER BY date) AS running_total
FROM (
    SELECT 
        DATE(order_purchase_timestamp) AS date,
        SUM(price) AS daily_sales
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    GROUP BY DATE(order_purchase_timestamp)
) t;

# Task 3: Top Products

SELECT 
    product_id,
    total_sales,
    DENSE_RANK() OVER (ORDER BY total_sales DESC) AS rank
FROM (
    SELECT 
        product_id,
        SUM(price) AS total_sales
    FROM order_items
    GROUP BY product_id
) t;

# Task 4 : Customer Lifetime Value
SELECT 
    c.customer_id,
    SUM(oi.price) AS total_spend
FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.customer_id;

# Task 5:Customer Segmentation
SELECT 
    customer_id,
    total_spend,
    CASE 
        WHEN total_spend > 10000 THEN 'Gold'
        WHEN total_spend >= 5000 THEN 'Silver'
        ELSE 'Bronze'
    END AS segment
FROM (
    SELECT 
        c.customer_id,
        SUM(oi.price) AS total_spend
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    JOIN customers c ON o.customer_id = c.customer_id
    GROUP BY c.customer_id
) t;

# Task 6: Final Reporting Table

SELECT 
    c.customer_id,
    c.customer_city,
    SUM(oi.price) AS total_spend,
    COUNT(DISTINCT o.order_id) AS total_orders,
    CASE 
        WHEN SUM(oi.price) > 10000 THEN 'Gold'
        WHEN SUM(oi.price) >= 5000 THEN 'Silver'
        ELSE 'Bronze'
    END AS segment
FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_city;
