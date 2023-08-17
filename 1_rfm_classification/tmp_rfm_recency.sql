CREATE TABLE analysis.tmp_rfm_recency (
 user_id INT NOT NULL PRIMARY KEY,
 recency INT NOT NULL CHECK(recency >= 1 AND recency <= 5)
);
INSERT INTO analysis.tmp_rfm_recency
SELECT id user_id,
		case when max_dt is null then 1 
			else ntile(5) over (order by max_dt asc) end recency
FROM analysis.users u
LEFT JOIN (SELECT user_id, 
		   max(order_ts) max_dt
     	   FROM analysis.orders o
      	   WHERE status = (SELECT id 
			   FROM analysis.orderstatuses 
			   WHERE lower(key) = 'closed')
		   and order_ts >= '2022-01-01'
           GROUP BY user_id) o
on u.id = o.user_id;
