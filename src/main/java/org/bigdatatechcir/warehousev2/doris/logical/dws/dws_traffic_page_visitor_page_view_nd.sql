-- 流量域访客页面粒度页面浏览最近n日汇总表
INSERT INTO dws.dws_traffic_page_visitor_page_view_nd(mid_id, k1, brand, model, operate_system, page_id, during_time_7d, view_count_7d, during_time_30d, view_count_30d)
select
    mid_id,
    k1,
    brand,
    model,
    operate_system,
    page_id,
    sum(if(k1>=date_add(date('${pdate}'),-6),during_time_1d,0)),
    sum(if(k1>=date_add(date('${pdate}'),-6),view_count_1d,0)),
    sum(during_time_1d),
    sum(view_count_1d)
from dws.dws_traffic_page_visitor_page_view_1d
where k1>=date_add(date('${pdate}'),-29)
  and k1<=date('${pdate}')
group by mid_id,k1,brand,model,operate_system,page_id;