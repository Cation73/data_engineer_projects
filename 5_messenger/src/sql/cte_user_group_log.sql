with user_group_log as (
            select hg.hk_group_id,
                count(distinct luga.hk_user_id) cnt_added_users
            from (select hg.hk_group_id
                    from MRSOKOLOVVVYANDEXRU__DWH.h_groups hg
                    order by hg.registration_dt ASC
                    limit 10) hg
            left join MRSOKOLOVVVYANDEXRU__DWH.l_users_group_activity luga
                on hg.hk_group_id = luga.hk_group_id
            right join MRSOKOLOVVVYANDEXRU__DWH.s_auth_history sah
                on luga.hk_l_user_group_activity = sah.hk_l_user_group_activity
            where 1=1
                and sah.event = 'add'
            group by hg.hk_group_id)

select hk_group_id
        ,cnt_added_users
from user_group_log
order by cnt_added_users
limit 10