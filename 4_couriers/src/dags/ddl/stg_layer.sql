drop table if exists stg.api_restaurants;
drop table if exists stg.api_deliveries;
drop table if exists stg.api_couriers;

create table if not exists stg.api_restaurants 
( 
id serial primary key, 
object_id varchar,
object_value text,
update_ts timestamp with time zone
);


create table if not exists stg.api_deliveries 
( 
id serial primary key, 
object_id varchar,
object_value text,
update_ts timestamp with time zone
);


create table if not exists stg.api_couriers 
( 
id serial primary key, 
object_id varchar,
object_value text,
update_ts timestamp with time zone
);