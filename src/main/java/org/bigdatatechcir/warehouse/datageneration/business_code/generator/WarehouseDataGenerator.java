package org.bigdatatechcir.warehouse.datageneration.business_code.generator;

import org.bigdatatechcir.warehouse.datageneration.business_code.util.DbUtil;
import org.bigdatatechcir.warehouse.datageneration.business_code.util.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class WarehouseDataGenerator {
    private static final Logger logger = LoggerFactory.getLogger(WarehouseDataGenerator.class);

    public static void generateWarehouseData(int batchSize) {
        generateWareInfo(batchSize);
        generateWareSku(batchSize);
        generateWareOrderTask(batchSize);
        generateWareOrderTaskDetail(batchSize);
    }

    private static void generateWareInfo(int batchSize) {
        String sql = "INSERT INTO ware_info (id, name, address, areacode) VALUES (?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            String name = "仓库" + i;
            String address = RandomUtil.generateAddress();
            String areacode = String.format("%06d", i);

            params.add(new Object[]{i, name, address, areacode});
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateWareSku(int batchSize) {
        String sql = "INSERT INTO ware_sku (id, sku_id, warehouse_id, stock, stock_name, stock_locked) " +
                    "VALUES (?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            String skuId = RandomUtil.generateSku();
            int warehouseId = RandomUtil.generateNumber(1, 100);
            int stock = RandomUtil.generateNumber(100, 1000);
            String stockName = "库存" + i;
            int stockLocked = RandomUtil.generateNumber(0, 50);

            params.add(new Object[]{i, skuId, warehouseId, stock, stockName, stockLocked});
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateWareOrderTask(int batchSize) {
        String sql = "INSERT INTO ware_order_task (id, order_id, consignee, consignee_tel, delivery_address, " +
                    "order_comment, payment_way, task_status, order_body, tracking_no, create_time, " +
                    "warehouse_id, task_comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            String orderId = RandomUtil.generateOrderNo();
            String consignee = RandomUtil.generateName();
            String consigneeTel = RandomUtil.generatePhone();
            String deliveryAddress = RandomUtil.generateAddress();
            String orderComment = "订单备注" + i;
            String paymentWay = "1";
            String taskStatus = String.valueOf(RandomUtil.generateNumber(1, 4));
            String orderBody = "商品描述" + i;
            String trackingNo = RandomUtil.generateUUID().substring(0, 12);
            LocalDateTime createTime = LocalDateTime.now();
            int warehouseId = RandomUtil.generateNumber(1, 100);
            String taskComment = "任务备注" + i;

            params.add(new Object[]{
                i, orderId, consignee, consigneeTel, deliveryAddress, orderComment,
                paymentWay, taskStatus, orderBody, trackingNo, createTime,
                warehouseId, taskComment
            });
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateWareOrderTaskDetail(int batchSize) {
        String sql = "INSERT INTO ware_order_task_detail (id, sku_id, sku_name, sku_num, task_id, " +
                    "warehouse_id, status) VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            String skuId = RandomUtil.generateSku();
            String skuName = "商品" + i;
            int skuNum = RandomUtil.generateNumber(1, 5);
            int taskId = RandomUtil.generateNumber(1, batchSize);
            int warehouseId = RandomUtil.generateNumber(1, 100);
            String status = String.valueOf(RandomUtil.generateNumber(1, 4));

            params.add(new Object[]{
                i, skuId, skuName, skuNum, taskId, warehouseId, status
            });
        }
        DbUtil.batchInsert(sql, params);
    }
} 