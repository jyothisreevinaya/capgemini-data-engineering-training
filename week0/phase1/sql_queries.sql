# Schema SQL
CREATE TABLE customers (
 customer_id INT,
 customer_name VARCHAR(50),
 city VARCHAR(50),
 age INT
);
INSERT INTO customers VALUES
(1, 'Ravi', 'Hyderabad', 25),
(2, 'Sita', 'Chennai', 32),
(3, 'Arun', 'Hyderabad', 28);
# Queries SQL
select * from customers;
select * from customers where city = 'Chennai';
select * from customers where age>25;
select customer_name, city from customers;
select city, count(*) from customers group by city;
