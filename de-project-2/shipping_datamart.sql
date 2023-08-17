CREATE OR REPLACE VIEW public.shipping_datamart AS
select si.shippingid,
		si.vendorid,
		st.transfer_type,
		-- date_part('day', age(ss.shipping_end_fact_datetime, ss.shipping_start_fact_datetime))::bigint full_day_at_shipping,
		extract(day from (ss.shipping_end_fact_datetime - ss.shipping_start_fact_datetime))::bigint full_day_at_shipping,
		case when ss.shipping_end_fact_datetime > si.shipping_plan_datetime then 1 
				else 0 end is_delay,
		case when ss.status = 'finished' then 1 
				else 0 end is_shipping_finish,
		shipping_end_fact_datetime, shipping_plan_datetime,
		case when ss.shipping_end_fact_datetime > si.shipping_plan_datetime 
					-- then date_part('day', age(ss.shipping_end_fact_datetime, si.shipping_plan_datetime))::bigint
					then extract(day from (ss.shipping_end_fact_datetime - si.shipping_plan_datetime))::bigint
				else 0 end delay_day_at_shipping,
		si.payment_amount,
		si.payment_amount*(scr.shipping_country_base_rate + sa.agreement_rate + st.shipping_transfer_rate) vat,
		si.payment_amount*sa.agreement_commission profit  
from public.shipping_info si
inner join public.shipping_status ss
on ss.shippingid = si.shippingid
left join public.shipping_transfer st
on si.transfer_type_id = st.transfer_type_id
left join public.shipping_agreement sa
on si.agreementid = sa.agreementid
left join public.shipping_country_rates scr
on si.shipping_country_id = scr.shipping_country_id
