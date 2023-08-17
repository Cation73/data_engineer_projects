# 1.3. Качество данных

## Оцените, насколько качественные данные хранятся в источнике.
Опишите, как вы проверяли исходные данные и какие выводы сделали.

1. production.orderitems:
- проверено количество уникальных product_id, оно совпадает с id из production.products;
- проверено count(distinct discount) - одно уникальное значение (ноль), 
возможно, что это поле подтягивается некорректно;

2. production.orders:
- проверено количество уникальных order_id, оно совпадает с order_id из production.orderitems;
- проверено количество уникальных user_id, оно совпадает с id из production.users;
- в таблице всего 2 уникальных статуса - закрыт и отменен, это только облегчает задачу;
- проверены min/max bonus_payment, cost, payment - тут всё в пределах нормы;
- проверены min/max order_ts - данные взяты за месяц;
- проверено count(distinct bonus_payment) - одно уникальное значение (ноль), 
возможно, что это поле подтягивается некорректно;
- есть подозрение неправильного формирования cost (CONSTRAINT orders_check CHECK ((cost = (payment + bonus_payment))))
судя из названий, возможно, должно быть так payment = cost - bonus_payment (нужна методология формирования);

3. production.users - есть подозрение, что перепутаны названия name и login,
для login (ФИО) не стандартизирован (есть случаи, когда не только ФИО).

## Укажите, какие инструменты обеспечивают качество данных в источнике.
Ответ запишите в формате таблицы со следующими столбцами:
- `Наименование таблицы` - наименование таблицы, объект которой рассматриваете.
- `Объект` - Здесь укажите название объекта в таблице, на который применён инструмент. Например, здесь стоит перечислить поля таблицы, индексы и т.д.
- `Инструмент` - тип инструмента: первичный ключ, ограничение или что-то ещё.
- `Для чего используется` - здесь в свободной форме опишите, что инструмент делает.

Пример ответа:

| Таблицы             | Объект                      | Инструмент      | Для чего используется |
| ------------------- | --------------------------- | --------------- | --------------------- |
| production.products | id int NOT NULL PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность записей о продуктах |
| production.products | CONSTRAINT products_price_check CHECK ((price >= (0)::numeric)) | Ограничения - проверки | Позволяет избежать ошибок, что цена принимает отрицательное значение и тип данных является нечисленным |
|  |  |  |  |
| production.users | id int NOT NULL PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность записей о пользователях |
| production.users | login varchar(2048) NOT NULL | Ограничение NOT NULL | Обеспечивает верное заполнение основного столбца в таблице - для каждого id должен быть логин |
|  |  |  |  |
| production.orderitems | id int4 NOT NULL GENERATED ALWAYS AS IDENTITY, CONSTRAINT orderitems_pkey PRIMARY KEY (id) | Первичный ключ  | Обеспечивает уникальность записей о заказах |
| production.orderitems | CONSTRAINT orderitems_check CHECK (((discount >= (0)::numeric) AND (discount <= price))) | Ограничения - проверки |  Позволяет избежать ошибок, что скидка принимает отрицательное значение и может быть больше цены, и тип данных является нечисленным |
| production.orderitems | CONSTRAINT orderitems_order_id_product_id_key UNIQUE (order_id, product_id) | Ограничения уникальности  | Ограничение указывает, что сочетание значений перечисленных столбцов будет уникальным |
| production.orderitems | CONSTRAINT orderitems_price_check CHECK ((price >= (0)::numeric)) | Ограничения - проверки | Позволяет избежать ошибок, что цена принимает отрицательное значение и тип данных является нечисленным|
| production.orderitems | CONSTRAINT orderitems_quantity_check CHECK ((quantity > 0)) | Ограничения - проверки  | Позволяет избежать ошибок, что количество товаров больше 0 (нет смысла заносить в базу отрицательные и нулевые значения) |
| production.orderitems | CONSTRAINT orderitems_order_id_fkey FOREIGN KEY (order_id) REFERENCES production.orders(order_id) | Внешний ключ |  Обеспечивает ссылочную целостность двух связанных таблиц (orderitems и orders) |
| production.orderitems | CONSTRAINT orderitems_product_id_fkey FOREIGN KEY (product_id) REFERENCES production.products(id) | Внешний ключ |  Обеспечивает ссылочную целостность двух связанных таблиц (orderitems и products) |
| production.orderitems | column type_data NOT NULL | Ограничение NOT NULL  | Обеспечивает ненулевые значения во всех столбцах таблицы |
|  |  |  |  |
| production.orderstatuses | id int NOT NULL PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность записей о пользователях |
| production.orderstatuses | column type_data NOT NULL  | Ограничение NOT NULL | Обеспечивает ненулевые значения во всех столбцах таблицы |
|  |  |  |  |
| production.orders | order_id int NOT NULL PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность записей о пользователях |
| production.orders | CONSTRAINT orders_check CHECK ((cost = (payment + bonus_payment))) | Ограничения - проверки | Позволяет избежать ошибок, что cost будет больше или меньше, чем сумма payment и bonus_payment |
| production.orders | column type_data NOT NULL  | Ограничение NOT NULL | Обеспечивает ненулевые значения во всех столбцах таблицы |


