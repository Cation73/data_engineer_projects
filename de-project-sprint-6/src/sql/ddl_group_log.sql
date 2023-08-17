
drop table if exists MRSOKOLOVVVYANDEXRU__STAGING.group_log;

create table if not exists MRSOKOLOVVVYANDEXRU__STAGING.group_log
(group_id int primary key not null,
user_id int,
user_id_from  int,
event varchar(100),
event_dt timestamp)

order by group_id
partition by event_dt::date
group by calendar_hierarchy_day(event_dt::date, 3, 2)
;