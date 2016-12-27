select _id,title,createtime from liveshow where createtime >=
to_char(to_timestamp('{UOW_FROM}','YYYYMMDDHH24MISS'),'yyyy-mm-dd hh24:mis:ss') 