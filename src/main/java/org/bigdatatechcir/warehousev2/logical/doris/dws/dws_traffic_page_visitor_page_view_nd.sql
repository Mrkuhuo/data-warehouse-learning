INSERT INTO dws.dws_traffic_page_visitor_page_view_nd(mid_id, k1, brand, model, operate_system, page_id, during_time_7d, view_count_7d, during_time_30d, view_count_30d)
select
    mid_id,
    CURRENT_DATE(),
    brand,
    model,
    operate_system,
    page_id,
    sum(during_time_1d),
    sum(view_count_1d),
    sum(during_time_1d),
    sum(view_count_1d)
from dws.dws_traffic_page_visitor_page_view_1d
group by mid_id,brand,model,operate_system,page_id;