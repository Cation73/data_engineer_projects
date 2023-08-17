-- для теста
-- DROP TABLE IF EXISTS public.shipping_country_rates;
-- создадим таблицу
CREATE TABLE IF NOT EXISTS public.shipping_country_rates (
	shipping_country_id SERIAL PRIMARY KEY,
	shipping_country varchar(40) UNIQUE,
	shipping_country_base_rate numeric(14, 3)
);
-- заполним таблицу
INSERT INTO public.shipping_country_rates 
select row_number() over() shipping_country_id, 
		shipping_country, 
		shipping_country_base_rate
from de.public.shipping
group by shipping_country, 
		 shipping_country_base_rate;
