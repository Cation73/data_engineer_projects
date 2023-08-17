insert into dds.dm_restaurants (restaurant_id, restaurant_name)
select restaurant_id,
		restaurant_name
from stg.api_restaurants
where update_ts::Date = {{date}};