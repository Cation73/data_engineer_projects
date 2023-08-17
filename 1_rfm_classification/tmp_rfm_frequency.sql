CREATE TABLE analysis.tmp_rfm_frequency (
 user_id INT NOT NULL PRIMARY KEY,
 frequency INT NOT NULL CHECK(frequency >= 1 AND frequency <= 5)
);
INSERT INTO analysis.tmp_rfm_frequency
SELECT id user_id, 
	   ntile(5) over (order by coalesce(quantity, 0) asc) frequency
FROM analysis.users u
LEFT JOIN (SELECT user_id, sum(quantity) quantity
	  		FROM analysis.orders o
	  		LEFT JOIN (SELECT order_id, sum(quantity) quantity
				 		FROM analysis.orderitems
				 		GROUP BY order_id) oi 
	  		ON o.order_id = oi.order_id
	  		WHERE status = (SELECT id 
			  				FROM analysis.orderstatuses 
			  				WHERE lower(key) = 'closed')
		   			and order_ts >= '2022-01-01'
	  		GROUP BY user_id) o
on u.id = o.user_id;
