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
public class OrderDetailGenerator {
    private static final Logger logger = LoggerFactory.getLogger(OrderDetailGenerator.class);
    private static final int BATCH_SIZE = 500;

    @Autowired
    private DbUtil dbUtil;

    public void generateOrderDetailData(int count) {
        generateOrderDetail(count);
        generateOrderDetailActivity(count);
        generateOrderDetailCoupon(count);
    }

    private void generateOrderDetail(int count) {
        String sql = "INSERT INTO order_detail (id, order_id, sku_id, sku_name, img_url, order_price, " +
                    "sku_num, create_time, source_type, source_id, split_total_amount, " +
                    "split_activity_amount, split_coupon_amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        int remainingCount = count;
        while (remainingCount > 0) {
            // 获取最大ID
            String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM order_detail";
            int startId = dbUtil.queryForInt(maxIdSql) + 1;
            
            int batchCount = Math.min(remainingCount, BATCH_SIZE);
            List<Object[]> params = new ArrayList<>();
            
            for (int i = 0; i < batchCount; i++) {
                int id = startId + i;
                int orderId = RandomUtil.generateNumber(1, 1000);
                int skuId = RandomUtil.generateNumber(1, 1000);
                String skuName = generateRandomSkuName();
                String imgUrl = "http://example.com/products/sku" + skuId + ".jpg";
                double orderPrice = RandomUtil.generateDouble(50, 1000);
                int skuNum = RandomUtil.generateNumber(1, 5);
                LocalDateTime now = LocalDateTime.now();
                int sourceType = RandomUtil.generateNumber(1, 3);
                int sourceId = RandomUtil.generateNumber(1, 100);
                double splitTotalAmount = orderPrice * skuNum;
                double splitActivityAmount = RandomUtil.generateDouble(5, 100);
                double splitCouponAmount = RandomUtil.generateDouble(5, 50);
                
                params.add(new Object[]{
                    id, orderId, skuId, skuName, imgUrl, orderPrice, skuNum, now, sourceType, sourceId,
                    splitTotalAmount, splitActivityAmount, splitCouponAmount
                });
            }
            
            dbUtil.batchInsert(sql, params);
            remainingCount -= batchCount;
        }
    }

    private void generateOrderDetailActivity(int count) {
        String sql = "INSERT INTO order_detail_activity (id, order_id, order_detail_id, activity_id, " +
                    "activity_rule_id, sku_id, create_time) VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        int remainingCount = count;
        while (remainingCount > 0) {
            // 获取最大ID
            String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM order_detail_activity";
            int startId = dbUtil.queryForInt(maxIdSql) + 1;
            
            int batchCount = Math.min(remainingCount, BATCH_SIZE);
            List<Object[]> params = new ArrayList<>();
            
            for (int i = 0; i < batchCount; i++) {
                int id = startId + i;
                int orderId = RandomUtil.generateNumber(1, 1000);
                int orderDetailId = RandomUtil.generateNumber(1, 1000);
                int activityId = RandomUtil.generateNumber(1, 100);
                int activityRuleId = RandomUtil.generateNumber(1, 100);
                int skuId = RandomUtil.generateNumber(1, 1000);
                LocalDateTime now = LocalDateTime.now();
                
                params.add(new Object[]{
                    id, orderId, orderDetailId, activityId, activityRuleId, skuId, now
                });
            }
            
            dbUtil.batchInsert(sql, params);
            remainingCount -= batchCount;
        }
    }

    private void generateOrderDetailCoupon(int count) {
        String sql = "INSERT INTO order_detail_coupon (id, order_id, order_detail_id, coupon_id, " +
                    "coupon_use_id, sku_id, create_time) VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        int remainingCount = count;
        while (remainingCount > 0) {
            // 获取最大ID
            String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM order_detail_coupon";
            int startId = dbUtil.queryForInt(maxIdSql) + 1;
            
            int batchCount = Math.min(remainingCount, BATCH_SIZE);
            List<Object[]> params = new ArrayList<>();
            
            for (int i = 0; i < batchCount; i++) {
                int id = startId + i;
                int orderId = RandomUtil.generateNumber(1, 1000);
                int orderDetailId = RandomUtil.generateNumber(1, 1000);
                int couponId = RandomUtil.generateNumber(1, 100);
                int couponUseId = RandomUtil.generateNumber(1, 1000);
                int skuId = RandomUtil.generateNumber(1, 1000);
                LocalDateTime now = LocalDateTime.now();
                
                params.add(new Object[]{
                    id, orderId, orderDetailId, couponId, couponUseId, skuId, now
                });
            }
            
            dbUtil.batchInsert(sql, params);
            remainingCount -= batchCount;
        }
    }

    private String generateRandomSkuName() {
        String[] styles = {"简约", "潮流", "经典", "时尚", "优雅", "复古", "现代", "自然", "奢华", "清新"};
        String[] types = {"T恤", "裤子", "外套", "连衣裙", "衬衫", "鞋子", "包包", "帽子", "围巾", "手套"};
        
        String style = styles[RandomUtil.generateNumber(0, styles.length - 1)];
        String type = types[RandomUtil.generateNumber(0, types.length - 1)];
        
        return style + type;
    }
} 