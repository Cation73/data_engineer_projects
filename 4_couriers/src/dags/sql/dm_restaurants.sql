select object_id restaurant_id,
		object_value::json ->> 'name' restaurant_name
from stg.api_restaurants;