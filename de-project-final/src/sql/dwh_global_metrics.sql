INSERT INTO MRSOKOLOVVVYANDEXRU__DWH.global_metrics (date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
    with filter_data as (select transaction_dt::date date_update
							, t.currency_code currency_from
							, account_number_from
							, operation_id 
							, case when amount < 0 then amount*-1*c.currency_with_div else amount*c.currency_with_div end amount 
							, c.currency_code_with 
					from MRSOKOLOVVVYANDEXRU__STAGING.transactions t
					left join MRSOKOLOVVVYANDEXRU__STAGING.currencies c 
					on t.transaction_dt::date = c.date_update::date 
						and t.currency_code  = c.currency_code 
						and c.currency_code_with = 420
					where 1=1
						and status = 'done'
						and account_number_from > 0
						and account_number_to  > 0)
    select date_update
            , currency_from
            , sum(amount) amount_total
            , count(distinct operation_id) cnt_transactions
            , (sum(amount) / count(distinct operation_id))::numeric(14,2) avg_transactions_per_account
            , count(distinct account_number_from) cnt_accounts_make_transactions 
    from filter_data
    where 1=1
        and date_update > (select max(date_update) from MRSOKOLOVVVYANDEXRU__DWH.global_metrics)
    group by date_update,
            currency_from
    order by date_update,
                currency_from