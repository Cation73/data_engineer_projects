drop table if exists MRSOKOLOVVVYANDEXRU__DWH.global_metrics ;

create table if not exists MRSOKOLOVVVYANDEXRU__DWH.global_metrics
(date_update timestamp not null,
currency_from int,
amount_total  numeric(14, 2),
cnt_transactions int,
avg_transactions_per_account numeric(14, 2), 
cnt_accounts_make_transactions int)

order by date_update
partition by date_update::date
group by calendar_hierarchy_day(date_update::date, 3, 2)
;

create projection MRSOKOLOVVVYANDEXRU__DWH.global_metrics
(
 date_update,
 currency_from,
 amount_total,
 cnt_transactions,
 avg_transactions_per_account,
 cnt_accounts_make_transactions
)
as
 select global_metrics.date_update,
        global_metrics.currency_from,
        global_metrics.amount_total,
        global_metrics.cnt_transactions,
        global_metrics.avg_transactions_per_account,
        global_metrics.cnt_accounts_make_transactions
 from MRSOKOLOVVVYANDEXRU__DWH.global_metrics
 order by hash(global_metrics.date_update, global_metrics.currency_from)
 segmented by hash(global_metrics.date_update, global_metrics.currency_from) ALL NODES KSAFE 1;


select MARK_DESIGN_KSAFE(1);