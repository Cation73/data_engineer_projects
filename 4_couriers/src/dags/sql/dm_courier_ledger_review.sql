insert into cdm.dm_courier_ledger (courier_id, courier_name, settlement_year, settlement_month, orders_count,
									orders_total_sum, rate_avg, order_processing_fee, courier_order_sum,
									courier_tips_sum, courier_reward_sum)
select t.*,
		courier_order_sum + courier_tips_sum * 0.95 courier_reward_sum
from (select dc.courier_id,
		dc.courier_name,
		dt."year" settlement_year,
		dt."month" settlement_month,
		count(distinct dd.order_id) orders_count,
		sum(dd.sum) orders_total_sum,
		avg(dd.rate) rate_avg,
		0.25*sum(dd.tip_sum) order_processing_fee,
		case when avg(dd.rate) < 4 and 0.05*sum(dd.tip_sum) > 100 then 0.05*sum(dd.tip_sum) 
			when avg(dd.rate) < 4 and 0.05*sum(dd.tip_sum) <= 100 then 100 
			when avg(dd.rate) between 4 and 4.5 and 0.07*sum(dd.tip_sum) > 150 then 0.07*sum(dd.tip_sum) 
			when avg(dd.rate) between 4 and 4.5 and 0.07*sum(dd.tip_sum) <= 150 then 150
			when avg(dd.rate) between 4.5  and 4.9 and 0.08*sum(dd.tip_sum) > 175 then 0.08*sum(dd.tip_sum) 
			when avg(dd.rate) between 4.5  and 4.9 and 0.08*sum(dd.tip_sum) <= 175 then 175
			when avg(dd.rate) >= 4.9 and 0.1*sum(dd.tip_sum) > 200 then 0.1*sum(dd.tip_sum) 
			when avg(dd.rate) >= 4.9 and 0.1*sum(dd.tip_sum) <= 200 then 200
			end courier_order_sum,
		sum(dd.tip_sum) courier_tips_sum
from dds.dm_deliveries  dd
left join dds.dm_couriers dc 
on dd.courier_id = dc.id 
left join dds.dm_timestamps dt 
on dd.timestamp_id = dt.id 
where dt."month" = extract(month from ({{date}} - interval '1 month'))::int
	and dt."year" >= extract(year from ({{date}} - interval '1 month'))::int
group by dc.courier_id,
		dc.courier_name,
		dt."year", 
		dt."month") t
