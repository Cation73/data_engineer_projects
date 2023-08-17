INSERT INTO analysis.dm_rfm_segments
SELECT user_id,
	recency, 
	frequency,
	monetary_value
FROM (SELECT r.user_id, 
		recency, 
		frequency,
		monetary_value
      FROM analysis.tmp_rfm_recency r
      INNER JOIN analysis.tmp_rfm_frequency f
      ON r.user_id = f.user_id
      INNER JOIN analysis.tmp_rfm_monetary_value m
      ON r.user_id = m.user_id) rfm;
SELECT *
FROM analysis.dm_rfm_segments
ORDER BY user_id asc
LIMIT 10;
/*user_id recency frequency monetary_value
0	1	4	4
1	4	3	3
2	2	5	5
3	2	3	3
4	4	3	3
5	5	5	5
6	1	5	5
7	4	2	2
8	1	3	3
9	1	2	2*/
