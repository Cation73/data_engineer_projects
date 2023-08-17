select date_part('year', ts::timestamp) "year",
       date_part('month', ts::timestamp) "month",
       date_part('day', ts::timestamp) "day",
       cast(ts::timestamp as time) "time",
       cast(ts::timestamp as date) "date"
from (select object_value::json ->> 'order_ts' ts
		from stg.api_deliveries) ad;