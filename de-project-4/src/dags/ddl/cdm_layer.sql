create table cdm.dm_courier_ledger
(id serial primary key not null,
courier_id int not null,
courier_name text not null, 
settlement_year int not null,
settlement_month int not null,
orders_count int default 0 not null,
orders_total_sum numeric(14, 2) not null,
rate_avg numeric(14, 2) not null,
order_processing_fee numeric(14, 2) not null,
courier_order_sum numeric(14, 2) not null,
courier_tips_sum numeric(14, 2) not null,
courier_reward_sum numeric(14, 2) not null
);


alter table cdm.dm_courier_ledger 
   add constraint dm_courier_ledger_orders_count_check 
   check (orders_count >= 0);

alter table cdm.dm_courier_ledger 
   add constraint dm_courier_ledger_orders_total_sum_check 
   check (orders_total_sum >= 0);
alter table cdm.dm_courier_ledger 
   alter column orders_total_sum set default 0;

alter table cdm.dm_courier_ledger 
   add constraint dm_courier_ledger_courier_order_sum_check 
   check (courier_order_sum >= 0);
alter table cdm.dm_courier_ledger 
   alter column courier_order_sum set default 0;

alter table cdm.dm_courier_ledger 
   add constraint dm_courier_ledger_courier_tips_sum_check 
   check (courier_tips_sum >= 0);
alter table cdm.dm_courier_ledger 
   alter column courier_tips_sum set default 0;

alter table cdm.dm_courier_ledger 
   add constraint dm_courier_ledger_order_processing_fee_check 
   check (order_processing_fee >= 0);
alter table cdm.dm_courier_ledger 
   alter column order_processing_fee set default 0;

alter table cdm.dm_courier_ledger 
   add constraint dm_courier_ledger_courier_reward_sum_check 
   check (courier_reward_sum >= 0);
alter table cdm.dm_courier_ledger 
   alter column courier_reward_sum set default 0;

alter table cdm.dm_courier_ledger 
   add constraint dm_courier_ledger_settlement_monthcheck 
   check (settlement_month >= 1 and settlement_month <= 12);

alter table cdm.dm_courier_ledger 
   add constraint dm_courier_ledger_settlement_year_check 
   check (settlement_year >= 1950 and settlement_year <= 2500);

alter table cdm.dm_courier_ledger
   add unique (courier_id, settlement_month, settlement_year);
