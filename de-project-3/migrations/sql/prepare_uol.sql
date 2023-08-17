-- очистим от возможных дублей при заливке данных в staging.tmp_user_order_log
DELETE FROM staging.tmp_user_order_log
WHERE id IN (SELECT id
              FROM (SELECT id,
                             ROW_NUMBER() OVER (partition BY date_time, 
								city_id, 
								customer_id,
								item_id,
								quantity,
								payment_amount,
								status
								ORDER BY id) AS rnum
                     FROM staging.tmp_user_order_log) t
              WHERE t.rnum > 1);
-- вставка данных на втором уровне stage
INSERT INTO staging.user_order_log
SELECT id,
	date_time,
	city_id,
	city_name,
	customer_id,
	first_name,
	last_name,
	item_id,
	item_name,
	quantity,
	case when status = 'refunded' then payment_amount*(-1)
		 when status = 'shipped' then payment_amount
		 end payment_amount,
	case when status is not null then status
			else 'shipped' end status
FROM staging.tmp_user_order_log;
-- очистим таблицу первого уровня stage
DELETE FROM staging.tmp_user_order_log;
