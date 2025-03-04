package org.bigdatatechcir.warehouse.datageneration.business_code.generator;

import org.bigdatatechcir.warehouse.datageneration.business_code.util.DbUtil;
import org.bigdatatechcir.warehouse.datageneration.business_code.util.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Component
public class OrderStatusLogGenerator {
    private static final Logger logger = LoggerFactory.getLogger(OrderStatusLogGenerator.class);

    @Autowired
    private DbUtil dbUtil;

    public void generateOrderStatusLog(int count) {
        // 获取最大ID
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM order_status_log";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO order_status_log (id, order_id, order_status, operate_time) " +
                    "VALUES (?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            // 从已有订单中选择
            int orderId = RandomUtil.generateNumber(1, 4863); // 根据order_info表的AUTO_INCREMENT值
            String orderStatus = String.valueOf(RandomUtil.generateNumber(1001, 1006)); // 根据base_dic表中的状态码
            LocalDateTime operateTime = LocalDateTime.now();
            
            params.add(new Object[]{id, orderId, orderStatus, operateTime});
        }
        dbUtil.batchInsert(sql, params);
    }
} 