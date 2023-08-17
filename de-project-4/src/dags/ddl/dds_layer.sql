drop table if exists dds.dm_deliveries cascade;
drop table if exists dds.dm_couriers cascade;
drop table if exists dds.dm_timestamps cascade;


create table dds.dm_couriers 
( 
id serial primary key, 
courier_id varchar not null,
courier_name varchar not null
);

create table dds.dm_timestamps
(
id    serial primary key,
ts    timestamp not null,
year  integer   not null
        constraint dm_timestamps_year_check
            check ((year >= 2022) AND (year < 2500)),
month integer   not null
        constraint dm_timestamps_month_check
            check ((month >= 1) AND (month <= 12)),
day   integer   not null
        constraint dm_timestamps_day_check
            check ((day >= 1) AND (day <= 31)),
time  time      not null,
date  date      not null
);

create table dds.dm_deliveries
( 
id serial primary key, 
order_id varchar not null, 
timestamp_id int not null,
delivery_id varchar not null,
courier_id int not null,
address text not null,
delivery_ts timestamp with time zone not null,
rate int default 0 not null, 
sum numeric(14,2) default 0 not null, 
tip_sum numeric(14,2) default 0 not null
);

alter table dds.dm_deliveries 
add constraint fk_dm_deliveries_timestamps
foreign key (timestamp_id) 
references dds.dm_timestamps (id)
on delete cascade;

alter table dds.dm_deliveries 
add constraint fk_dm_deliveries_couriers
foreign key (courier_id) 
references dds.dm_couriers (id)
on delete cascade;
