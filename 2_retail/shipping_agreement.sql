-- для теста
-- DROP TABLE IF EXISTS public.shipping_agreement;
-- создание таблицы
CREATE TABLE IF NOT EXISTS public.shipping_agreement (
	agreementid BIGINT PRIMARY KEY,
	agreement_number varchar(40),
	agreement_rate numeric(14, 3),
        agreement_commission numeric(14, 3)
);
-- заполнение таблицы
INSERT INTO public.shipping_agreement 
select distinct description[1]::bigint agreementid,
		description[2]::varchar(20) agreement_number,
		description[3]::numeric(14,3) agreement_rate,
		description[4]::numeric(14,3) agreement_commission
from (select regexp_split_to_array(vendor_agreement_description, ':') description 
		from de.public.shipping) d
order by agreementid;
