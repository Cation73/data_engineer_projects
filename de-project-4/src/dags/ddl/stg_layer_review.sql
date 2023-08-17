drop table if exists stg.api_restaurants;
drop table if exists stg.api_deliveries;
drop table if exists stg.api_couriers;

create table if not exists stg.api_restaurants 
( 
id serial primary key, 
restaurant_id varchar,
restaurant_name text,
update_ts timestamp with time zone
);


create table if not exists stg.api_deliveries 
( 
id serial primary key, 
order_id varchar,
delivery_id varchar,
courier_id varchar,
address text,
delivery_ts timestamp with time zone,
rate int,
sum numeric(14,2),
tip_sum numeric(14,2),
update_ts timestamp with time zone
);


create table if not exists stg.api_couriers 
( 
id serial primary key, 
courier_id varchar,
courier_name text,
update_ts timestamp with time zone
);
