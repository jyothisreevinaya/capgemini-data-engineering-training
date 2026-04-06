## Create Table
CREATE TABLE customers (
    customer_id INT,
    name VARCHAR(50),
    city VARCHAR(50),
    age INT
);
## Insert Data
INSERT INTO customers (customer_id, name, city, age) VALUES
(1, 'Ravi', 'Hyderabad', 25),
(2, NULL, 'Chennai', 32),
(NULL, 'Arun', 'Hyderabad', 28),
(4, 'Meena', NULL, 30),
(4, 'Meena', NULL, 30),
(5, 'John', 'Bangalore', -5);

## View Initial Data
SELECT * FROM customers;

## Data Cleaning (create new cleaned table)
CREATE TABLE cleaned_customers AS
SELECT DISTINCT
    customer_id,
    COALESCE(name, 'Unknown') AS name,
    COALESCE(city, 'Unknown') AS city,
    age
FROM customers
WHERE customer_id IS NOT NULL
  AND age > 0;

## View Cleaned Data
SELECT * FROM cleaned_customers;

## Validation
SELECT 
    (SELECT COUNT(*) FROM customers) AS before_cleaning,
    (SELECT COUNT(*) FROM cleaned_customers) AS after_cleaning;

## Aggregation
SELECT city, COUNT(*) AS customer_count
FROM cleaned_customers
GROUP BY city;
