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
public class WarehouseDataGenerator {
    private static final Logger logger = LoggerFactory.getLogger(WarehouseDataGenerator.class);
    private static final int BATCH_SIZE = 500;

    @Autowired
    private DbUtil dbUtil;

    public void generateWarehouseData(int count) {
        logger.info("开始生成仓库数据...");
        
        logger.info("正在更新 ware_info 表...");
        generateWareInfo(count);
        
        logger.info("正在更新 ware_sku 表...");
        generateWareSku(count);
        
        logger.info("正在更新 ware_order_task 表...");
        generateWareOrderTask(count);
        
        logger.info("正在更新 ware_order_task_detail 表...");
        generateWareOrderTaskDetail(count);
        
        logger.info("仓库数据生成完成");
    }

    private void generateWareInfo(int count) {
        String sql = "INSERT INTO ware_info (id, name, address, areacode) VALUES (?, ?, ?, ?)";
        
        int remainingCount = count;
        while (remainingCount > 0) {
            // 获取最大ID
            String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM ware_info";
            int startId = dbUtil.queryForInt(maxIdSql) + 1;
            
            int batchCount = Math.min(remainingCount, BATCH_SIZE);
            List<Object[]> params = new ArrayList<>();
            
            for (int i = 0; i < batchCount; i++) {
                int id = startId + i;
                String name = "仓库" + id;
                String address = RandomUtil.generateAddress();
                String areacode = String.format("%06d", RandomUtil.generateNumber(100000, 999999));
                
                params.add(new Object[]{
                    id, name, address, areacode
                });
            }
            
            dbUtil.batchInsert(sql, params);
            remainingCount -= batchCount;
        }
    }

    private void generateWareSku(int count) {
        String sql = "INSERT INTO ware_sku (id, sku_id, warehouse_id, stock, stock_name, stock_locked) " +
                    "VALUES (?, ?, ?, ?, ?, ?)";
        
        int remainingCount = count;
        while (remainingCount > 0) {
            // 获取最大ID
            String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM ware_sku";
            int startId = dbUtil.queryForInt(maxIdSql) + 1;
            
            int batchCount = Math.min(remainingCount, BATCH_SIZE);
            List<Object[]> params = new ArrayList<>();
            
            for (int i = 0; i < batchCount; i++) {
                int id = startId + i;
                int skuId = RandomUtil.generateNumber(1, 35);
                int warehouseId = RandomUtil.generateNumber(1, 10);
                int stock = RandomUtil.generateNumber(100, 1000);
                String stockName = "库存" + id;
                int stockLocked = RandomUtil.generateNumber(0, 100);
                
                params.add(new Object[]{
                    id, skuId, warehouseId, stock, stockName, stockLocked
                });
            }
            
            dbUtil.batchInsert(sql, params);
            remainingCount -= batchCount;
        }
    }

    private void generateWareOrderTask(int count) {
        String sql = "INSERT INTO ware_order_task (id, order_id, consignee, consignee_tel, delivery_address, " +
                    "order_comment, payment_way, task_status, order_body, tracking_no, create_time, " +
                    "ware_id, task_comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        int remainingCount = count;
        while (remainingCount > 0) {
            // 获取最大ID
            String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM ware_order_task";
            int startId = dbUtil.queryForInt(maxIdSql) + 1;
            
            int batchCount = Math.min(remainingCount, BATCH_SIZE);
            List<Object[]> params = new ArrayList<>();
            
            for (int i = 0; i < batchCount; i++) {
                int id = startId + i;
                int orderId = RandomUtil.generateNumber(1, 5000);
                String consignee = RandomUtil.generateName();
                String consigneeTel = RandomUtil.generatePhone();
                String deliveryAddress = RandomUtil.generateAddress();
                String orderComment = "订单备注" + id;
                int paymentWay = RandomUtil.generateNumber(1, 2);
                int taskStatus = RandomUtil.generateNumber(1001, 1006);
                String orderBody = "订单内容" + id;
                String trackingNo = "TN" + String.format("%010d", id);
                LocalDateTime now = LocalDateTime.now();
                int wareId = RandomUtil.generateNumber(1, 10);
                String taskComment = "任务备注" + id;
                
                params.add(new Object[]{
                    id, orderId, consignee, consigneeTel, deliveryAddress, orderComment,
                    paymentWay, taskStatus, orderBody, trackingNo, now, wareId, taskComment
                });
            }
            
            dbUtil.batchInsert(sql, params);
            remainingCount -= batchCount;
        }
    }

    private void generateWareOrderTaskDetail(int count) {
        String sql = "INSERT INTO ware_order_task_detail (id, sku_id, sku_name, sku_num, task_id, refund_status) " +
                    "VALUES (?, ?, ?, ?, ?, ?)";
        
        int remainingCount = count;
        while (remainingCount > 0) {
            // 获取最大ID
            String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM ware_order_task_detail";
            int startId = dbUtil.queryForInt(maxIdSql) + 1;
            
            int batchCount = Math.min(remainingCount, BATCH_SIZE);
            List<Object[]> params = new ArrayList<>();
            
            for (int i = 0; i < batchCount; i++) {
                int id = startId + i;
                int skuId = RandomUtil.generateNumber(1, 35);
                String skuName = "商品" + skuId;
                int skuNum = RandomUtil.generateNumber(1, 10);
                int taskId = RandomUtil.generateNumber(-10, 0); // 使用负数作为任务ID
                String refundStatus = String.valueOf(RandomUtil.generateNumber(701, 706)); // 退款状态
                
                params.add(new Object[]{
                    id, skuId, skuName, skuNum, taskId, refundStatus
                });
            }
            
            dbUtil.batchInsert(sql, params);
            remainingCount -= batchCount;
        }
    }
} 