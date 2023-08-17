-- добавим новый столбец со статусом
-- для прошлых записем укажем, что статус = shipped
ALTER TABLE staging.user_order_log 
ADD COLUMN status VARCHAR(20) DEFAULT 'shipped';