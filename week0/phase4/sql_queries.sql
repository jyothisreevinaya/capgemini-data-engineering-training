## 1. EXTRACT (Create Tables)

CREATE TABLE customers (
    customer_id INT,
    customer_name VARCHAR(50),
    city VARCHAR(50),
    age INT
);

INSERT INTO customers VALUES
(1, 'Ravi', 'Hyderabad', 25),
(2, 'Sita', 'Chennai', 32),
(3, 'Arun', 'Hyderabad', 28),
(4, 'Meena', 'Bengaluru', 35),
(5, 'Kiran', 'Chennai', 22);


CREATE TABLE orders (
    order_id INT,
    customer_id INT,
    amount INT,
    order_date DATE
);

INSERT INTO orders VALUES
(101, 1, 2500, '2026-03-01'),
(102, 2, 1800, '2026-03-02'),
(103, 1, 3200, '2026-03-03'),
(104, 3, 1500, '2026-03-04'),
(105, 5, 2800, '2026-03-05');

## 2. TRANSFORM

## Data Cleaning
CREATE VIEW customers_clean AS
SELECT *
FROM customers
WHERE customer_id IS NOT NULL
  AND age >= 0;

CREATE VIEW orders_clean AS
SELECT *
FROM orders
WHERE customer_id IS NOT NULL
  AND order_id IS NOT NULL
  AND amount > 0;

## Daily Sales
CREATE VIEW daily_sales AS
SELECT order_date, SUM(amount) AS total_sales
FROM orders_clean
GROUP BY order_date;

## City-wise Revenue
CREATE VIEW city_revenue AS
SELECT c.city, SUM(o.amount) AS total_revenue
FROM customers_clean c
JOIN orders_clean o
ON c.customer_id = o.customer_id
GROUP BY c.city;

## Customers
CREATE VIEW top_customers AS
SELECT c.customer_name, SUM(o.amount) AS total_spend
FROM customers_clean c
JOIN orders_clean o
ON c.customer_id = o.customer_id
GROUP BY c.customer_name
ORDER BY total_spend DESC
LIMIT 5;

## Repeat Customers (>1 order)
CREATE VIEW repeat_customers AS
SELECT customer_id, COUNT(order_id) AS order_count
FROM orders_clean
GROUP BY customer_id
HAVING COUNT(order_id) > 1;

## Customer Segmentation
CREATE VIEW segmentation AS
SELECT c.customer_name,
       SUM(o.amount) AS total_spend,
       CASE
           WHEN SUM(o.amount) > 10000 THEN 'Gold'
           WHEN SUM(o.amount) BETWEEN 5000 AND 10000 THEN 'Silver'
           ELSE 'Bronze'
       END AS segment
FROM customers_clean c
JOIN orders_clean o
ON c.customer_id = o.customer_id
GROUP BY c.customer_name;

## Final Reporting Table
CREATE TABLE final_report AS
SELECT c.customer_id,
       c.customer_name,
       c.city,
       SUM(o.amount) AS total_spend,
       COUNT(o.order_id) AS order_count,
       CASE
           WHEN SUM(o.amount) > 10000 THEN 'Gold'
           WHEN SUM(o.amount) BETWEEN 5000 AND 10000 THEN 'Silver'
           ELSE 'Bronze'
       END AS segment
FROM customers_clean c
JOIN orders_clean o
ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name, c.city;


##  3. LOAD (Final Output)
SELECT * FROM final_report;

## SHOW RESULTS

## Daily Sales
SELECT * FROM daily_sales;

## City Revenue
SELECT * FROM city_revenue;

## Top Customers
SELECT * FROM top_customers;

## Repeat Customers
SELECT * FROM repeat_customers;

## Segmentation
SELECT * FROM segmentation;
