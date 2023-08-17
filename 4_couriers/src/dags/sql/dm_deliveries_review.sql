insert into dds.dm_deliveries (order_id, timestamp_id, delivery_id, courier_id, address, delivery_ts, rate, sum, tip_sum)
select order_id,
		dt.id timestamp_id, 
		delivery_id,
		dc.id courier_id, 
		address,
		delivery_ts::timestamptz,
		rate,
		sum,
		tip_sum
from stg.api_deliveries ad
inner join dds.dm_couriers dc 
on ad.courier_id = dc.courier_id
inner join dds.dm_timestamps dt 
on dt.ts = order_ts::timestamptz
where delivery_ts::Date = {{date}};
