# Витрина RFM

## 1.1. Выяснение требований к целевой витрине.

Задача: построить витрину для RFM-классификации.

Алгоритм:

Необходимо присвоить каждому клиенту три значения:

1. Фактор R (Recency) измеряется по последнему заказу: 1 получают те, кто либо вообще не делал заказов, либо делал очень давно; 5 получают те, кто заказывают недавно.
2. Фактор F (Frequency) - оценивается по количеству заказов: 1 - клиенты с наименьшим количеством заказов; 5 - с наибольшим.
3. Фактор M (Monetary) - оцениваются по потраченной сумме: 1 - клиенты с наименьшей суммой; 5 - с наибольшей.

Затем необходимо проверить распределение количества клиентов по сегментам.

Структура витрины:

1. user_id (integer)
2. recency (integer)
3. frequency (integer)
4. monetary_value (integer)

Период дат: c 01.01.2022 по н.в. 

Обновление витрины: нет потребности в обновлении витрины.

Название схемы и полученной витрины: analysis.dm_rfm_segments


## 1.2. Изучение структуры исходных данных.

production.orders:
1. user_id - id пользователя
2. order_ts - дата и время 
3. payment - оплата (с учетом оплаты бонусами) - возможно нужен cost
4. status - id статуса
5. order_id - id заказа

production.orderitems:
1. order_id - id заказа
2. quantity - количество заказанных позиций
3. price - цена (возможно, для проверки)

production.orderstatuses
1. id - id статуса
2. key - название статуса

## 1.3. Анализ качества данных

Для проведения первичной RFM-классификации данные качественные (подробнее в data_quality.md), 
применен целый пул ограничений для заполнения таблиц, что и позволяет заполнять витрины качественными данными.

Использовались следующие ограничения для обеспечения качества данных в таблицах:
1. NOT NULL
2. PRIMARY KEY
3. CONSTRAINT name_column CHECK condition
4. UNIQUE 
5. FOREIGN KEY


## 1.4. Подготовка витрины данных

### 1.4.1. Создание VIEW для таблиц из базы production.**

```SQL
-- view orderitems
CREATE OR REPLACE VIEW analysis.orderitems AS
SELECT * 
FROM production.orderitems;

-- view orders
CREATE OR REPLACE VIEW analysis.orders AS
SELECT * 
FROM production.orders;

--view orderstatuses
CREATE OR REPLACE VIEW analysis.orderstatuses AS
SELECT * 
FROM production.orderstatuses;

-- view products
CREATE OR REPLACE VIEW analysis.products AS
SELECT * 
FROM production.products;

-- view users
CREATE OR REPLACE VIEW analysis.users AS
SELECT * 
FROM production.users;
```

### 1.4.2. Написание DDL-запрос для создания витрины.**

```SQL
-- table for rfm-classification
CREATE TABLE analysis.dm_rfm_segments (
user_id INT NOT NULL,
recency INT NOT NULL,
frequency INT NOT NULL,
monetary_value INT NOT NULL,
CONSTRAINT dm_rfm_segments_pkey PRIMARY KEY (user_id),
CONSTRAINT dm_rfm_segments_recency_check CHECK (recency >= 1 AND recency <= 5),
CONSTRAINT dm_rfm_segments_frequency_check CHECK (frequency >= 1 AND frequency <= 5),
CONSTRAINT dm_rfm_segments_monetary_check CHECK (monetary_value >= 1 AND monetary_value <= 5)
);
```

### 1.4.3. Написание SQL запрос для заполнения витрины

```SQL
-- recency
insert into analysis.tmp_rfm_recency 
select user_id, ntile(5) over (order by diff_day asc) recency
from (select user_id, date_part('day', current_date - max(order_ts)) diff_day
	  from analysis.orders o
	  -- по логике - нужно рассматривать только те заказы, 
	  -- которые были успешны (были закрыты)
	  where status = (select id 
			  from analysis.orderstatuses 
			  where lower(key) = 'closed')
	  group by user_id) r
-- frequency
insert into analysis.tmp_rfm_frequency
select user_id, 
	ntile(5) over (order by quantity asc) frequency
from (select user_id, sum(quantity) quantity
	  from analysis.orders o
	  left join (select order_id, sum(quantity) quantity
				 from analysis.orderitems
				 group by order_id) oi 
	  on o.order_id = oi.order_id
	  -- по логике - нужно рассматривать только те заказы, 
	  -- которые были успешны (были закрыты)
	  where status = (select id 
			  from production.orderstatuses 
			  where lower(key) = 'closed')
	  group by user_id) f;
-- monetary_value
insert into analysis.tmp_rfm_monetary_value
select user_id, 
	ntile(5) over (order by sum_cost asc) monetary
from (select user_id, sum(cost) sum_cost
	  from analysis.orders o
          -- по логике - нужно рассматривать только те заказы, 
	  -- которые были успешны (были закрыты)
	  where status = (select id 
			  from analysis.orderstatuses 
			  where lower(key) = 'closed')
	  group by user_id) m;
-- insert to result table 
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
```



