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
, user_group_messages as (
                select hg.hk_group_id,
                    count(distinct lum.hk_user_id) cnt_users_in_group_with_messages
                from MRSOKOLOVVVYANDEXRU__DWH.h_groups hg
                left join MRSOKOLOVVVYANDEXRU__DWH.l_groups_dialogs lgd
                    on hg.hk_group_id = lgd.hk_group_id
                left join MRSOKOLOVVVYANDEXRU__DWH.l_user_message lum
                    on lgd.hk_message_id = lum.hk_message_id
                group by hg.hk_group_id)

select ugl.hk_group_id,
        ugl.cnt_added_users,
        ugm.cnt_users_in_group_with_messages,
        ugm.cnt_users_in_group_with_messages / nullif(ugl.cnt_added_users, 0) group_conversion
from user_group_log as ugl
left join user_group_messages as ugm
    on ugl.hk_group_id = ugm.hk_group_id
order by ugm.cnt_users_in_group_with_messages / nullif(ugl.cnt_added_users, 0) desc