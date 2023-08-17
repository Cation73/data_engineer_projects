drop table if exists MRSOKOLOVVVYANDEXRU__STAGING.currencies;

create table if not exists MRSOKOLOVVVYANDEXRU__STAGING.currencies
(currency_code int primary key not null,
currency_code_with int,
date_update  timestamp,
currency_with_div numeric(14, 2))

order by currency_code
partition by date_update::date
group by calendar_hierarchy_day(date_update::date, 3, 2);


create projection MRSOKOLOVVVYANDEXRU__STAGING.currencies 
(
 currency_code,
 currency_code_with,
 date_update,
 currency_with_div
)
as
 select currencies.currency_code,
        currencies.currency_code_with,
        currencies.date_update,
        currencies.currency_with_div
 from MRSOKOLOVVVYANDEXRU__STAGING.currencies
 order by hash(currencies.currency_code, currencies.date_update)
 segmented BY hash(currencies.currency_code, currencies.date_update) ALL NODES KSAFE 1;


select MARK_DESIGN_KSAFE(1);