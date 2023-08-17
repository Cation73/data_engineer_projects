select object_id order_id,
		dt.id timestamp_id, 
		object_value::json ->> 'delivery_id' delivery_id,
		dc.id courier_id, 
		object_value::json ->> 'address' address,
		(object_value::json ->> 'delivery_ts')::timestamptz delivery_ts,
		object_value::json ->> 'rate' rate,
		object_value::json ->> 'sum' sum,
		object_value::json ->> 'tip_sum' tip_sum
from stg.api_deliveries ad
inner join dds.dm_couriers dc 
on ad.courier_id = dc.courier_id
inner join dds.dm_timestamps dt 
on dt.ts = (object_value::json ->> 'order_ts')::timestamptz;