INSERT INTO dws.dws_traffic_page_visitor_page_view_1d(mid_id, k1, brand, model, operate_system, page_id, during_time_1d, view_count_1d)
select
    mid_id,
    CURRENT_DATE(),
    brand,
    model,
    operate_system,
    page_id,
    sum(during_time),
    count(*)
from dwd_traffic_page_view_inc
group by mid_id,brand,model,operate_system,page_id;