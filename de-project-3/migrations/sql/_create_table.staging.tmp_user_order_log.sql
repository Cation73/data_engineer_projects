-- создание первого уровня staging
create table staging.tmp_user_order_log as
select *
from staging.user_order_log 
where 1=0;