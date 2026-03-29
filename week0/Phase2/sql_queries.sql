#SQL Schema
CREATE TABLE customers (
    customer_id INT,
    customer_name VARCHAR(50),
    city VARCHAR(50),
    age INT
);
INSERT INTO customers (customer_id, customer_name, city, age) VALUES
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
INSERT INTO orders (order_id, customer_id, amount, order_date) VALUES
(101, 1, 2500, '2026-03-01'),
(102, 2, 1800, '2026-03-02'),
(103, 1, 3200, '2026-03-03'),
(104, 3, 1500, '2026-03-04'),
(105, 5, 2800, '2026-03-05');
#Queries
select c.customer_id, c.customer_name, sum(o.amount) as total_amount
from customers c
join orders o
on c.customer_id = o.customer_id
group by c.customer_id, c.customer_name;
#Query 2
select c.customer_id, c.customer_name, sum(o.amount) as total_amount
from customers c
join orders o
on c.customer_id = o.customer_id
group by c.customer_id, c.customer_name
order by total_amount desc
limit 3;
#Query 3
select c.customer_id, c.customer_name
from customers c
left join orders o
on c.customer_id = o.customer_id
where o.customer_id is null;
#Query 4
select c.city, sum(o.amount) as total_revenue
from customers c
join orders o
on c.customer_id = o.customer_id
group by c.city;
#Query 5
select c.customer_id, c.customer_name, avg(o.amount) as avg_amount
from customers c
join orders o
on c.customer_id = o.customer_id
group by c.customer_id, c.customer_name;
#Query 6
select customer_id, count(*) as order_count
from orders o
group by customer_id
having count(*) > 1;
#Query 7
select c.customer_id, c.customer_name, sum(o.amount) as total_amount
from customers c
join orders o
on c.customer_id = o.customer_id
group by c.customer_id, c.customer_name
order by total_amount desc;
