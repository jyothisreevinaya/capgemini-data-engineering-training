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

# Query 1(Read sales → clean nulls → calculate daily sales)

select order_date, sum(amount) as daily_sales
from orders
where amount is not null
group by order_date;

# Query 2(Read customers → clean invalid rows → city-wise revenue)

select c.city, sum(o.amount) as revenue
from customers c
join orders o
on c.customer_id = o.customer_id
where c.customer_id is not null
and c.city is not null
group by c.city;

# Query 3(Find repeat customers (>2 orders))

select customer_id, count(*) as order_count
from orders
group by customer_id
having count(*) > 2;

# Query 4(Highest spending customer in each city)

select t.city, t.customer_id, t.total_spend
from (
  select c.city, c.customer_id, sum(o.amount) total_spend
  from customers c join orders o
  on c.customer_id=o.customer_id
  group by c.city, c.customer_id
) t
join (
  select city, max(total_spend) max_spend
  from (
    select c.city, c.customer_id, sum(o.amount) total_spend
    from customers c join orders o
    on c.customer_id=o.customer_id
    group by c.city, c.customer_id
  ) x group by city
) m
on t.city=m.city and t.total_spend=m.max_spend;

#Query 5(Final Reporting Table)

select c.customer_id,
       ifnull(c.customer_name,'Unknown') as customer_name,
       ifnull(c.city,'Unknown') as city,
       sum(o.amount) as total_spend,
       count(o.order_id) as order_count
from customers c
join orders o
on c.customer_id = o.customer_id
where c.customer_id is not null
and c.age >= 0
group by c.customer_id, c.customer_name, c.city;
