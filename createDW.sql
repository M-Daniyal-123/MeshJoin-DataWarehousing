
drop schema if exists dwh;
create schema dwh;
use dwh;
drop table if exists sales_fact;
drop table if exists supplier;
drop table if exists customers;
drop table if exists products;
drop table if exists store;
drop table if exists time;
drop view if exists storeanalysis_mv;



create table customers(
customer_id varchar(6) primary key,
customer_name varchar(25)
);
create table products(
product_id varchar(6) primary key,
product_name VARCHAR(30) NOT NULL
);
create table store(
store_id VARCHAR(4) NOT NULL primary key, 
store_name VARCHAR(20) NOT NULL
);
create table time(
time_id int NOT NULL AUTO_INCREMENT primary key,
date_ date,
day varchar(15),
week_ int,
month_ varchar(15),
qtr_ int,
year_ int
);
create table supplier(
`SUPPLIER_ID` VARCHAR(5) primary key, 
`SUPPLIER_NAME` VARCHAR(30) NOT NULL
);

create table sales_fact(
time_id int,
product_id varchar(6),
store_id varchar(4),
customer_id varchar(6),
supplier_id varchar(6),
sales DECIMAL(5,2) DEFAULT (0.0) NOT NULL,
quantity int DEFAULT(0) NOT NULL,
constraint fk_supplier foreign key(supplier_id)
references supplier(supplier_id),
constraint fk_time foreign key(time_id)
references time(time_id),
constraint fk_store foreign key(store_id)
references store(store_id),
constraint fk_product foreign key(product_id)
references products(product_id),
constraint fk_customer foreign key(customer_id)
references customers(customer_id),
primary key (time_id,product_id,store_id,customer_id,supplier_id)
);


