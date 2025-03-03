package org.bigdatatechcir.warehouse.datageneration.business_code.generator;

import org.bigdatatechcir.warehouse.datageneration.business_code.util.DbUtil;
import org.bigdatatechcir.warehouse.datageneration.business_code.util.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class OrderDataGenerator {
    private static final Logger logger = LoggerFactory.getLogger(OrderDataGenerator.class);

    public static void generateOrderData(int batchSize) {
        generateOrderInfo(batchSize);
        generateOrderDetail(batchSize);
        generatePaymentInfo(batchSize);
        generateOrderStatusLog(batchSize);
        generateOrderDetailActivity(batchSize);
        generateOrderDetailCoupon(batchSize);
        generateOrderRefundInfo(batchSize);
        generateRefundPayment(batchSize);
    }

    private static void generateOrderInfo(int batchSize) {
        String sql = "INSERT INTO order_info (id, consignee, consignee_tel, total_amount, order_status, user_id, " +
                    "payment_way, delivery_address, order_comment, out_trade_no, trade_body, create_time, expire_time, " +
                    "process_status, tracking_no, parent_order_id, img_url, province_id) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            String orderId = RandomUtil.generateOrderNo();
            String consignee = RandomUtil.generateName();
            String phone = RandomUtil.generatePhone();
            BigDecimal totalAmount = RandomUtil.generatePrice(100, 10000);
            String orderStatus = "1"; // 1:未支付
            int userId = RandomUtil.generateNumber(1, 1000);
            String paymentWay = "1"; // 1:支付宝
            String address = RandomUtil.generateAddress();
            String comment = "订单备注" + i;
            String outTradeNo = RandomUtil.generateUUID();
            String tradeBody = "商品描述" + i;
            LocalDateTime createTime = LocalDateTime.now();
            LocalDateTime expireTime = createTime.plusHours(24);
            String processStatus = "1";
            String trackingNo = RandomUtil.generateUUID().substring(0, 12);
            String parentOrderId = "0";
            String imgUrl = "http://example.com/images/" + i + ".jpg";
            int provinceId = RandomUtil.generateNumber(1, 34);

            params.add(new Object[]{
                orderId, consignee, phone, totalAmount, orderStatus, userId, paymentWay, address,
                comment, outTradeNo, tradeBody, createTime, expireTime, processStatus,
                trackingNo, parentOrderId, imgUrl, provinceId
            });
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateOrderDetail(int batchSize) {
        String sql = "INSERT INTO order_detail (id, order_id, sku_id, sku_name, img_url, order_price, " +
                    "sku_num, create_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            String orderId = RandomUtil.generateOrderNo();
            String skuId = RandomUtil.generateSku();
            String skuName = "商品" + i;
            String imgUrl = "http://example.com/images/" + skuId + ".jpg";
            BigDecimal orderPrice = RandomUtil.generatePrice(10, 1000);
            int skuNum = RandomUtil.generateNumber(1, 5);
            LocalDateTime createTime = LocalDateTime.now();

            params.add(new Object[]{
                i, orderId, skuId, skuName, imgUrl, orderPrice, skuNum, createTime
            });
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generatePaymentInfo(int batchSize) {
        String sql = "INSERT INTO payment_info (id, out_trade_no, order_id, user_id, payment_type, " +
                    "trade_no, total_amount, subject, payment_status, create_time, callback_time) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            String outTradeNo = RandomUtil.generateUUID();
            String orderId = RandomUtil.generateOrderNo();
            int userId = RandomUtil.generateNumber(1, 1000);
            String paymentType = "1"; // 1:支付宝
            String tradeNo = RandomUtil.generateUUID();
            BigDecimal totalAmount = RandomUtil.generatePrice(100, 10000);
            String subject = "订单支付";
            String paymentStatus = "1"; // 1:未支付
            LocalDateTime createTime = LocalDateTime.now();
            LocalDateTime callbackTime = createTime.plusMinutes(RandomUtil.generateNumber(1, 30));

            params.add(new Object[]{
                i, outTradeNo, orderId, userId, paymentType, tradeNo, totalAmount,
                subject, paymentStatus, createTime, callbackTime
            });
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateOrderStatusLog(int batchSize) {
        String sql = "INSERT INTO order_status_log (id, order_id, order_status, operate_time) " +
                    "VALUES (?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            String orderId = RandomUtil.generateOrderNo();
            String orderStatus = String.valueOf(RandomUtil.generateNumber(1, 5));
            LocalDateTime operateTime = LocalDateTime.now();

            params.add(new Object[]{i, orderId, orderStatus, operateTime});
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateOrderDetailActivity(int batchSize) {
        String sql = "INSERT INTO order_detail_activity (id, order_id, order_detail_id, activity_id, activity_rule_id, " +
                    "sku_id, create_time) VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            String orderId = RandomUtil.generateOrderNo();
            int orderDetailId = RandomUtil.generateNumber(1, batchSize);
            int activityId = RandomUtil.generateNumber(1, 100);
            int activityRuleId = RandomUtil.generateNumber(1, 100);
            String skuId = RandomUtil.generateSku();
            LocalDateTime createTime = LocalDateTime.now();

            params.add(new Object[]{
                i, orderId, orderDetailId, activityId, activityRuleId, skuId, createTime
            });
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateOrderDetailCoupon(int batchSize) {
        String sql = "INSERT INTO order_detail_coupon (id, order_id, order_detail_id, coupon_id, coupon_use_id, " +
                    "sku_id, create_time) VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            String orderId = RandomUtil.generateOrderNo();
            int orderDetailId = RandomUtil.generateNumber(1, batchSize);
            int couponId = RandomUtil.generateNumber(1, 100);
            int couponUseId = RandomUtil.generateNumber(1, 100);
            String skuId = RandomUtil.generateSku();
            LocalDateTime createTime = LocalDateTime.now();

            params.add(new Object[]{
                i, orderId, orderDetailId, couponId, couponUseId, skuId, createTime
            });
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateOrderRefundInfo(int batchSize) {
        String sql = "INSERT INTO order_refund_info (id, user_id, order_id, sku_id, refund_type, refund_num, " +
                    "refund_amount, refund_reason_type, refund_reason_txt, refund_status, create_time) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            int userId = RandomUtil.generateNumber(1, 1000);
            String orderId = RandomUtil.generateOrderNo();
            String skuId = RandomUtil.generateSku();
            String refundType = String.valueOf(RandomUtil.generateNumber(1, 2));
            int refundNum = RandomUtil.generateNumber(1, 5);
            BigDecimal refundAmount = RandomUtil.generatePrice(10, 1000);
            String refundReasonType = String.valueOf(RandomUtil.generateNumber(1, 5));
            String refundReasonTxt = "退款原因" + i;
            String refundStatus = String.valueOf(RandomUtil.generateNumber(1, 4));
            LocalDateTime createTime = LocalDateTime.now();

            params.add(new Object[]{
                i, userId, orderId, skuId, refundType, refundNum, refundAmount,
                refundReasonType, refundReasonTxt, refundStatus, createTime
            });
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateRefundPayment(int batchSize) {
        String sql = "INSERT INTO refund_payment (id, out_trade_no, order_id, sku_id, payment_type, trade_no, " +
                    "total_amount, subject, refund_status, create_time, callback_time) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            String outTradeNo = RandomUtil.generateUUID();
            String orderId = RandomUtil.generateOrderNo();
            String skuId = RandomUtil.generateSku();
            String paymentType = String.valueOf(RandomUtil.generateNumber(1, 3));
            String tradeNo = RandomUtil.generateUUID();
            BigDecimal totalAmount = RandomUtil.generatePrice(10, 1000);
            String subject = "退款" + i;
            String refundStatus = String.valueOf(RandomUtil.generateNumber(1, 3));
            LocalDateTime createTime = LocalDateTime.now();
            LocalDateTime callbackTime = createTime.plusMinutes(RandomUtil.generateNumber(1, 30));

            params.add(new Object[]{
                i, outTradeNo, orderId, skuId, paymentType, tradeNo, totalAmount,
                subject, refundStatus, createTime, callbackTime
            });
        }
        DbUtil.batchInsert(sql, params);
    }
} 