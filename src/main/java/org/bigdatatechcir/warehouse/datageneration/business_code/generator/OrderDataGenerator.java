package org.bigdatatechcir.warehouse.datageneration.business_code.generator;

import org.bigdatatechcir.warehouse.datageneration.business_code.util.DbUtil;
import org.bigdatatechcir.warehouse.datageneration.business_code.util.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Component
public class OrderDataGenerator {
    private static final Logger logger = LoggerFactory.getLogger(OrderDataGenerator.class);
    
    @Autowired
    private DbUtil dbUtil;

    public void generateOrderData(int batchSize) {
        generateOrderInfo(batchSize);
        generateOrderDetail(batchSize);
        generatePaymentInfo(batchSize);
        generateOrderStatusLog(batchSize);
        generateOrderDetailActivity(batchSize);
        generateOrderDetailCoupon(batchSize);
        generateOrderRefundInfo(batchSize);
        generateRefundPayment(batchSize);
    }

    private void generateOrderInfo(int count) {
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM order_info";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO order_info (id, consignee, consignee_tel, total_amount, " +
                    "order_status, user_id, payment_way, delivery_address, order_comment, " +
                    "out_trade_no, trade_body, create_time, expire_time, process_status, " +
                    "tracking_no, parent_order_id, img_url, province_id, activity_reduce_amount, " +
                    "coupon_reduce_amount, original_total_amount, feight_fee, refundable_time) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            LocalDateTime now = LocalDateTime.now();
            
            BigDecimal originalAmount = RandomUtil.generatePrice(150, 1500);  // 原价
            BigDecimal activityReduce = RandomUtil.generatePrice(0, 50);   // 活动优惠
            BigDecimal couponReduce = RandomUtil.generatePrice(0, 100);  // 优惠券优惠
            BigDecimal feightFee = RandomUtil.generatePrice(10, 50);     // 运费
            BigDecimal totalAmount = originalAmount.subtract(activityReduce)
                                                 .subtract(couponReduce)
                                                 .add(feightFee);
            
            params.add(new Object[]{
                id,
                "收货人" + id,
                "138" + RandomUtil.generateNumber(10000000, 99999999),
                totalAmount,
                "UNPAID",  // 订单状态：未支付
                RandomUtil.generateNumber(1, 1000),  // 用户ID
                "ALIPAY",  // 支付方式：支付宝
                "北京市朝阳区xxx街道" + id + "号",
                "订单备注" + id,
                String.format("ORDER%d%d", System.currentTimeMillis(), id),  // 商户订单号
                "商品描述" + id,
                now,
                now.plusDays(1),  // 24小时内支付
                "UNPAID",  // 订单处理状态：未支付
                null,  // 物流单号（未发货）
                null,  // 父订单号（无拆单）
                "http://example.com/order" + id + ".jpg",
                RandomUtil.generateNumber(1, 34),  // 省份ID
                activityReduce,
                couponReduce,
                originalAmount,
                feightFee,
                now.plusDays(7)  // 7天内可退款
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateOrderDetail(int count) {
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM order_detail";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO order_detail (id, order_id, sku_id, sku_name, img_url, " +
                    "order_price, sku_num, create_time, source_type, source_id, " +
                    "split_total_amount, split_activity_amount, split_coupon_amount) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();
        
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            BigDecimal orderPrice = RandomUtil.generatePrice(50, 500);
            int skuNum = RandomUtil.generateNumber(1, 5);
            BigDecimal splitTotal = orderPrice.multiply(new BigDecimal(skuNum));
            BigDecimal splitActivity = RandomUtil.generatePrice(0, 50);
            BigDecimal splitCoupon = RandomUtil.generatePrice(0, 100);
            
            params.add(new Object[]{
                id,
                RandomUtil.generateNumber(1, count),  // 关联订单
                RandomUtil.generateNumber(1, 1000),  // 商品SKU
                "商品" + id,
                "http://example.com/product" + id + ".jpg",
                orderPrice,
                skuNum,
                now,
                "APP",  // 来源类型：APP
                RandomUtil.generateNumber(1, 1000),  // 来源ID
                splitTotal,
                splitActivity,
                splitCoupon
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generatePaymentInfo(int count) {
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM payment_info";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO payment_info (id, out_trade_no, order_id, user_id, " +
                    "payment_type, trade_no, total_amount, subject, payment_status, " +
                    "create_time, callback_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();
        
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            String outTradeNo = String.format("PAY%d%d", System.currentTimeMillis(), id);
            int orderId = RandomUtil.generateNumber(1, count);
            int userId = RandomUtil.generateNumber(1, 1000);
            BigDecimal totalAmount = RandomUtil.generatePrice(100, 10000);
            
            LocalDateTime createTime = now.minusMinutes(RandomUtil.generateNumber(1, 60));
            LocalDateTime callbackTime = createTime.plusMinutes(RandomUtil.generateNumber(1, 30));
            
            params.add(new Object[]{
                id,
                outTradeNo,
                orderId,
                userId,
                "ALIPAY",  // 支付方式：支付宝
                String.format("T%d%d", System.currentTimeMillis(), id),  // 支付宝交易号
                totalAmount,
                "订单支付",
                "PAID",  // 支付状态：已支付
                createTime,
                callbackTime
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateOrderStatusLog(int count) {
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM order_status_log";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO order_status_log (id, order_id, order_status, operate_time) " +
                    "VALUES (?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();
        
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            params.add(new Object[]{
                id,
                RandomUtil.generateNumber(1, count),  // 关联订单
                "UNPAID",  // 订单状态：未支付
                now
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateOrderDetailActivity(int count) {
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM order_detail_activity";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO order_detail_activity (id, order_id, order_detail_id, " +
                    "activity_id, activity_rule_id, sku_id, create_time) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();
        
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            params.add(new Object[]{
                id,
                RandomUtil.generateNumber(1, count),  // 关联订单
                RandomUtil.generateNumber(1, count),  // 关联订单详情
                RandomUtil.generateNumber(1, 100),   // 活动ID
                RandomUtil.generateNumber(1, 100),   // 活动规则ID
                RandomUtil.generateNumber(1, 1000),  // 商品SKU
                now
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateOrderDetailCoupon(int count) {
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM order_detail_coupon";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO order_detail_coupon (id, order_id, order_detail_id, " +
                    "coupon_id, coupon_use_id, sku_id, create_time) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();
        
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            params.add(new Object[]{
                id,
                RandomUtil.generateNumber(1, count),  // 关联订单
                RandomUtil.generateNumber(1, count),  // 关联订单详情
                RandomUtil.generateNumber(1, 100),   // 优惠券ID
                RandomUtil.generateNumber(1, 100),   // 优惠券使用记录ID
                RandomUtil.generateNumber(1, 1000),  // 商品SKU
                now
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateOrderRefundInfo(int count) {
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM order_refund_info";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO order_refund_info (id, user_id, order_id, sku_id, " +
                    "refund_type, refund_num, refund_amount, refund_reason_type, " +
                    "refund_reason_txt, refund_status, create_time) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();
        
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            int refundNum = RandomUtil.generateNumber(1, 5);
            BigDecimal refundAmount = RandomUtil.generatePrice(10, 1000);
            
            params.add(new Object[]{
                id,
                RandomUtil.generateNumber(1, 1000),  // 用户ID
                RandomUtil.generateNumber(1, count), // 关联订单
                RandomUtil.generateNumber(1, 1000),  // 商品SKU
                RandomUtil.generateNumber(1, 2),     // 退款类型：1-仅退款，2-退货退款
                refundNum,
                refundAmount,
                RandomUtil.generateNumber(1, 5),     // 退款原因类型
                "退款原因" + id,
                RandomUtil.generateNumber(1, 4),     // 退款状态：1-待审核，2-已审核，3-已退款，4-已拒绝
                now
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateRefundPayment(int count) {
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM refund_payment";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO refund_payment (id, out_trade_no, order_id, sku_id, " +
                    "payment_type, trade_no, total_amount, subject, refund_status, " +
                    "create_time, callback_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();
        
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            String outTradeNo = String.format("REF%d%d", System.currentTimeMillis(), id);
            int orderId = RandomUtil.generateNumber(1, count);
            int skuId = RandomUtil.generateNumber(1, 1000);
            BigDecimal totalAmount = RandomUtil.generatePrice(10, 1000);
            
            LocalDateTime createTime = now.minusMinutes(RandomUtil.generateNumber(1, 60));
            LocalDateTime callbackTime = createTime.plusMinutes(RandomUtil.generateNumber(1, 30));
            
            params.add(new Object[]{
                id,
                outTradeNo,
                orderId,
                skuId,
                "ALIPAY",  // 退款方式：支付宝
                String.format("R%d%d", System.currentTimeMillis(), id),  // 退款交易号
                totalAmount,
                "订单退款" + id,
                RandomUtil.generateNumber(1, 3),  // 退款状态：1-退款中，2-已退款，3-退款失败
                createTime,
                callbackTime
            });
        }
        dbUtil.batchInsert(sql, params);
    }
} 