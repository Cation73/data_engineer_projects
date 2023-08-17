-- для теста
DROP TABLE IF EXISTS public.shipping_info;
-- создание таблицы
CREATE TABLE IF NOT EXISTS public.shipping_info (
	shippingid BIGINT PRIMARY KEY,
	vendorid BIGINT,
	shipping_plan_datetime TIMESTAMP,
    payment_amount numeric(14, 2),
  	transfer_type_id BIGINT,
    shipping_country_id BIGINT,
    agreementid BIGINT,
    
    FOREIGN KEY (transfer_type_id) REFERENCES shipping_transfer(transfer_type_id) ON UPDATE CASCADE,
    FOREIGN KEY (shipping_country_id) REFERENCES shipping_country_rates(shipping_country_id) ON UPDATE CASCADE,
    FOREIGN KEY (agreementid) REFERENCES shipping_agreement(agreementid) ON UPDATE CASCADE
);
-- заливка данных
-- вопрос: при джойне с shipping_agreement по agreement_number заметил, что появляются дубли по shippingId.
-- поэтому решил добавить условие джоина и по agreement_rate - не уверен, что это верно, но от дублей это избавило 
INSERT INTO public.shipping_info
select distinct sh.shippingid, 
				sh.vendorid, 
				sh.shipping_plan_datetime, 
				sh.payment_amount,
				st.transfer_type_id,
				cr.shipping_country_id, 
				sa.agreementid
from de.public.shipping sh
left join public.shipping_transfer st
on (regexp_split_to_array(sh.shipping_transfer_description, ':'))[1] = st.transfer_type
	and (regexp_split_to_array(sh.shipping_transfer_description, ':'))[2] = st.transfer_model
left join public.shipping_country_rates cr 
on sh.shipping_country = cr.shipping_country
left join public.shipping_agreement sa
on (regexp_split_to_array(sh.vendor_agreement_description, ':'))[2] = sa.agreement_number
	and (regexp_split_to_array(sh.vendor_agreement_description, ':'))[3]::numeric(14,3) = sa.agreement_rate