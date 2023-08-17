insert into dds.dm_timestamps (ts, year, month, day, time, date)
select ts, 
       date_part('year', ts::timestamp) "year",
       date_part('month', ts::timestamp) "month",
       date_part('day', ts::timestamp) "day",
       cast(ts::timestamp as time) "time",
       cast(ts::timestamp as date) "date"
from (select order_ts ts
		from stg.api_deliveries
        where delivery_ts::Date = {{date}}) ad;