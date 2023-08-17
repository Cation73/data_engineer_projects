select object_id courier_id,
		object_value::json ->> 'name' courier_name
from stg.api_couriers;