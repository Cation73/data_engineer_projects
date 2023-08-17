-- удаление данных за прошлый период 
delete from mart.f_customer_retention
where period_name in (select week_of_year_iso
						from mart.d_calendar
						where date_actual = {{date}});
-- вставка данных в mart.f_customer_retention 
insert into mart.f_customer_retention 
with new_cust_count as (select  dense_rank() over (order by dc.week_of_year_iso) period_id,  
								dc.week_of_year_iso period_name,
					   			item_id,
								count(distinct customer_id) new_customers_count,
					   			sum(quantity * payment_amount) new_customers_revenue 
						from mart.f_sales fs
						left join mart.d_calendar as dc on fs.date_id = dc.date_id
						where customer_id in (select customer_id
											  from mart.f_sales
											  group by customer_id
											  having count(id) = 1)
						group by dc.week_of_year_iso, item_id),
						
return_cus_count as (select  dense_rank() over (order by dc.week_of_year_iso) period_id, 
								dc.week_of_year_iso period_name,
					 			item_id,
								count(distinct customer_id) returning_customers_count,
					 			sum(quantity * payment_amount) returning_customers_revenue  
						from mart.f_sales fs
						left join mart.d_calendar as dc on fs.date_id = dc.date_id
						where customer_id in (select customer_id
											  from staging.user_order_log 
											  group by customer_id
											  having count(id) > 1)
						group by dc.week_of_year_iso, item_id),	

refund_cust_count as (select  dense_rank() over (order by dc.week_of_year_iso) period_id,  
								dc.week_of_year_iso period_name,
					  			item_id,
								count(distinct customer_id) refunded_customer_count,
					  			count(*) customers_refunded 
						from mart.f_sales fs
						left join mart.d_calendar as dc on fs.date_id = dc.date_id
						where customer_id in (select customer_id
											  from staging.user_order_log
											  where status = 'refunded'
											  group by customer_id)
						group by dc.week_of_year_iso, item_id)
select 
		ncc.new_customers_count,
		rcc.returning_customers_count,
		recc.refunded_customer_count,
		ncc.period_name,
		ncc.period_id,
		ncc.item_id,
		ncc.new_customers_revenue,
		rcc.returning_customers_revenue,
		recc.customers_refunded
from new_cus_count ncc
inner join return_cus_count rcc 
on ncc.period_id = rcc.period_id and ncc.item_id = rcc.item_id
inner join refund_cust_count recc 
on ncc.period_id = recc.period_id and ncc.item_id = recc.item_id;