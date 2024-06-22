-- 流量域访客页面粒度页面浏览最近1日汇总表
INSERT INTO dws.dws_traffic_page_visitor_page_view_1d(mid_id, k1, brand, model, operate_system, page_id, during_time_1d, view_count_1d)
select
    mid_id,
    k1,
    brand,
    model,
    operate_system,
    page_id,
    sum(during_time),
    count(*)
from dwd.dwd_traffic_page_view_inc
group by mid_id,k1,brand,model,operate_system,page_id;