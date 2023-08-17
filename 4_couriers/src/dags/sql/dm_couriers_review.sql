insert into dds.dm_couriers (courier_id, courier_name)
select courier_id,
		courier_name
from stg.api_couriers
where update_ts::Date = {{date}};