CREATE TABLE analysis.tmp_rfm_monetary_value (
 user_id INT NOT NULL PRIMARY KEY,
 monetary_value INT NOT NULL CHECK(monetary_value >= 1 AND monetary_value <= 5)
);
INSERT INTO analysis.tmp_rfm_monetary_value 
SELECT id user_id, 
	   ntile(5) over (order by coalesce(sum_cost, 0) asc) monetary
FROM analysis.users u
LEFT JOIN (SELECT user_id, sum(cost) sum_cost
		   FROM analysis.orders o
		   WHERE status = (SELECT id 
				   FROM analysis.orderstatuses 
				   WHERE lower(key) = 'closed')
			and order_ts >= '2022-01-01'
		   GROUP BY user_id) o
on u.id = o.user_id
