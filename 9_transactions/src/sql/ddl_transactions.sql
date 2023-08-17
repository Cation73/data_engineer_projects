drop table if exists MRSOKOLOVVVYANDEXRU__STAGING.transactions;

create table if not exists MRSOKOLOVVVYANDEXRU__STAGING.transactions
(operation_id uuid primary key not null,
account_number_from int,
account_number_to  int,
currency_code int,
country varchar(100), 
status varchar(100),
transaction_type varchar(100),
amount int,
transaction_dt timestamp)

order by operation_id
partition by transaction_dt::date
group by calendar_hierarchy_day(transaction_dt::date, 3, 2)
;

create projection MRSOKOLOVVVYANDEXRU__STAGING.transactions
(
 operation_id,
 account_number_from,
 account_number_to,
 currency_code,
 country,
 status,
 transaction_type,
 amount,
 transaction_dt
)
as
 select transactions.operation_id,
        transactions.account_number_from,
        transactions.account_number_to,
        transactions.currency_code,
        transactions.country,
        transactions.status,
        transactions.transaction_type,
        transactions.amount,
        transactions.transaction_dt
 from MRSOKOLOVVVYANDEXRU__STAGING.transactions
 order by hash(transactions.operation_id, transactions.transaction_dt)
 segmented BY hash(transactions.operation_id, transactions.transaction_dt) ALL NODES KSAFE 1;

select MARK_DESIGN_KSAFE(1);