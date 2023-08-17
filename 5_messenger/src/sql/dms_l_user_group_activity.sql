insert into dwh.MRSOKOLOVVVYANDEXRU__DWH.l_users_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
select 
    hash(hu.hk_user_id, hg.hk_group_id) hk_l_user_group_activity,
    hu.hk_user_id,
    hg.hk_group_id,
    now() as load_dt,
    's3' as load_src
from dwh.MRSOKOLOVVVYANDEXRU__STAGING.group_log as gl 
left join dwh.MRSOKOLOVVVYANDEXRU__DWH.h_users as hu on  gl.user_id = hu.user_id
left join dwh.MRSOKOLOVVVYANDEXRU__DWH.h_groups as hg on gl.group_id = hg.group_id
where 1=1
    and hash(hu.hk_user_id, hg.hk_group_id) not in (select hk_l_user_group_activity from dwh.MRSOKOLOVVVYANDEXRU__DWH.l_users_group_activity);

