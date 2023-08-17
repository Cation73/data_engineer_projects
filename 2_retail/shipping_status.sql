-- для теста
DROP TABLE IF EXISTS public.shipping_status;
-- создание таблицы
CREATE TABLE IF NOT EXISTS public.shipping_status (
	shippingid BIGINT PRIMARY KEY,
	status text,
    state text, 
	shipping_start_fact_datetime TIMESTAMP,
    shipping_end_fact_datetime TIMESTAMP
);
-- заливка данных
INSERT INTO public.shipping_status
select shippingid,
		status,
		state,
		shipping_start_fact_datetime,
		shipping_end_fact_datetime
from (select shippingid,
		status,
		state,
		state_datetime,
		max(case when state = 'booked' then state_datetime end) over(partition by shippingid) shipping_start_fact_datetime,
		max(case when state = 'recieved' then state_datetime end) over(partition by shippingid) shipping_end_fact_datetime  
from public.shipping s
group by shippingid,
		status,
		state,
		state_datetime) t
where (shippingid, state_datetime) in (select shippingid,
	    										max(state_datetime) max_state_dt
										from public.shipping s
										group by shippingid);