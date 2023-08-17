-- для теста
-- DROP TABLE IF EXISTS public.shipping_transfer;
-- создание таблицы
CREATE TABLE IF NOT EXISTS public.shipping_transfer (
	transfer_type_id BIGINT PRIMARY KEY,
	transfer_type varchar(20),
	transfer_model text,
    shipping_transfer_rate numeric(14, 3)
);
-- заполнение таблицы
INSERT INTO public.shipping_transfer 
select  row_number() over (order by description[1]::varchar(20)) transfer_type_id, 
		description[1]::varchar(20) transfer_type,
		description[2]::text transfer_model,
		shipping_transfer_rate
from (select regexp_split_to_array(shipping_transfer_description, ':') description,
	  			shipping_transfer_rate
		from de.public.shipping) d
group by description[1],
		description[2],
		shipping_transfer_rate;
