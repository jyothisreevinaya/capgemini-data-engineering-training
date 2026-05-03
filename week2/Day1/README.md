# **1. Objective**

* To understand and implement **Subqueries** and **Common Table Expressions (CTEs)** in SQL.
* To perform **advanced data filtering, aggregation, and analysis** on employee, department, and order datasets.

# **2. Problem Summary**

* Basic SQL queries were not sufficient for:

  * Comparing values with **aggregated results (AVG, MIN, MAX)**
  * Filtering based on **related table data**
  * Writing **readable and reusable complex queries**
* Needed a way to:

  * Simplify complex logic
  * Avoid repeated queries
  * Improve query readability
  * 
# **3. Approach**

* Created sample tables:

  * **Employees1**
  * **Departments1**
  * **Orders1**

* Used:

  * **Subqueries** → for nested filtering
  * **CTEs (WITH clause)** → for modular and reusable query logic

# **4. Key Transformations Used**

## **A. Subqueries**

### 1. Salary greater than average salary

SELECT emp_id, name, salary 
FROM employees1 
WHERE salary > (SELECT AVG(salary) FROM employees1

### 2. Employees in same department as Alice


select e.emp_id, e.name, e.department_id 
from employees1 e 
where e.department_id = (
    select e.department_id 
    from employees1 e 
    where e.name="Alice"
)

### 3. Employee with minimum salary

select e.emp_id,e.name, e.salary 
from employees1 e 
where salary = (select min(e.salary) from employees1 e )

### 4. Employees who placed orders

select emp_id, name 
from employees1 
where emp_id IN (
    select emp_id 
    from orders1  
    group by emp_id 
    having count(order_id)>=1
)

### 5. Salary greater than max salary in IT department

select emp_id, name 
from employees1 
where salary > (
    select max(salary) 
    from employees1 
    where department_id=102
)

## **B. CTEs (Common Table Expressions)**

### 1. High salary employees

with high_salary as(
select * from employees where salary>50000)
select * from high_salary

### 2. Average salary per department

with avg_department as (
select dept_name, avg(salary) 
from Employees1 e  
join Departments1 d 
on e.department_id=d.dept_id 
group by dept_name)
select * from avg_department

### 3. Employee with department name


with department as (
select emp_id,name, dept_name 
from Employees1 e  
join Departments1 d 
on e.department_id=d.dept_id )
select * from department


### 4. Employees earning above department average


WITH cte_avg_salary AS (
    SELECT department_id, AVG(salary) AS avg_salary
    FROM Employees1
    GROUP BY department_id
)
SELECT e.emp_id, e.name, e.salary, e.department_id
FROM Employees1 e
JOIN cte_avg_salary c
ON e.department_id = c.department_id
WHERE e.salary > c.avg_salary;


# **5. Output**

* Identified:

  * High-performing employees
  * Department-wise salary insights
  * Employees contributing to orders
* Generated **clean analytical results** using nested logic

# **6. Challenges Faced**

* Understanding **when to use subquery vs CTE**
* Managing **nested queries readability**
* Writing correct **aggregation inside subqueries**
* Debugging alias and join issues

# **7. Learnings**

* Subqueries are useful for **quick filtering**
* CTEs improve:

  * **Readability**
  * **Reusability**
  * **Maintainability**
* Learned how to:

  * Combine **JOIN + GROUP BY + CTE**
  * Write **modular SQL queries**
  * Handle **complex conditions efficiently**

# **8. Files in This Project**

* "Subqueries and CTE.ipynb"
  * Table creation
  * Data insertion
  * Subquery examples
  * CTE implementations


