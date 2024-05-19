INSERT INTO dws.dws_traffic_session_page_view_1d(session_id, mid_id, k1, brand, model, operate_system, version_code, channel, during_time_1d, page_count_1d)
select
    session_id,
    mid_id,
    CURRENT_DATE(),
    brand,
    model,
    operate_system,
    version_code,
    channel,
    sum(during_time),
    count(*)
from dwd_traffic_page_view_inc
group by session_id,mid_id,brand,model,operate_system,version_code,channel;