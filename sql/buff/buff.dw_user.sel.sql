        select 
            id,
username ,
nick_name ,
password ,
password_salt ,
to_char(login_time,'yyyy-mm-dd hh24:mi:ss') ,
to_char(update_time,'yyyy-mm-dd hh24:mi:ss') ,
to_char(create_time,'yyyy-mm-dd hh24:mi:ss') ,
avatars ,
mobile_phone ,
gender ,
thirdparty_account ,
signup_type ,
thirdparty_platform ,
level,
experience,
area ,
intro ,
nation_code ,
thirdparty_icon_url ,
show_cover ,
is_superuser,
role,
following,
follower,
phone_encrypt ,
is_blocked 
from public."user"
where 
create_time>=to_timestamp('{UOW_FROM}','YYYYMMDDHH24MISS')
OR 
UPDATE_TIME>=TO_TIMESTAMP('{UOW_FROM}','YYYYMMDDHH24MISS')
