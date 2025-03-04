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
import java.util.UUID;

@Component
public class RefundGenerator {
    private static final Logger logger = LoggerFactory.getLogger(RefundGenerator.class);
    private static final int BATCH_SIZE = 500;

    @Autowired
    private DbUtil dbUtil;

    public void generateRefundData(int count) {
        generateOrderRefundInfo(count);
        generateRefundPayment(count);
    }

    private void generateOrderRefundInfo(int count) {
        String sql = "INSERT INTO order_refund_info (id, user_id, order_id, sku_id, refund_type, " +
                    "refund_num, refund_amount, refund_reason_type, refund_reason_txt, refund_status, " +
                    "create_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        int remainingCount = count;
        while (remainingCount > 0) {
            // 获取最大ID
            String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM order_refund_info";
            int startId = dbUtil.queryForInt(maxIdSql) + 1;
            
            int batchCount = Math.min(remainingCount, BATCH_SIZE);
            List<Object[]> params = new ArrayList<>();
            
            for (int i = 0; i < batchCount; i++) {
                int id = startId + i;
                int userId = RandomUtil.generateNumber(1, 1000);
                int orderId = RandomUtil.generateNumber(1, 1000);
                int skuId = RandomUtil.generateNumber(1, 1000);
                String refundType = String.valueOf(RandomUtil.generateNumber(1, 2));
                int refundNum = RandomUtil.generateNumber(1, 5);
                BigDecimal refundAmount = RandomUtil.generatePrice(10, 1000);
                String refundReasonType = String.valueOf(RandomUtil.generateNumber(1, 3));
                String refundReasonTxt = "退款原因" + id;
                String refundStatus = String.valueOf(RandomUtil.generateNumber(1, 3));
                LocalDateTime now = LocalDateTime.now();
                
                params.add(new Object[]{
                    id, userId, orderId, skuId, refundType, refundNum, refundAmount,
                    refundReasonType, refundReasonTxt, refundStatus, now
                });
            }
            
            dbUtil.batchInsert(sql, params);
            remainingCount -= batchCount;
        }
    }

    private void generateRefundPayment(int count) {
        String sql = "INSERT INTO refund_payment (id, out_trade_no, order_id, sku_id, payment_type, " +
                    "trade_no, total_amount, subject, refund_status, create_time, callback_time) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        int remainingCount = count;
        while (remainingCount > 0) {
            // 获取最大ID
            String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM refund_payment";
            int startId = dbUtil.queryForInt(maxIdSql) + 1;
            
            int batchCount = Math.min(remainingCount, BATCH_SIZE);
            List<Object[]> params = new ArrayList<>();
            
            for (int i = 0; i < batchCount; i++) {
                int id = startId + i;
                String outTradeNo = UUID.randomUUID().toString();
                int orderId = RandomUtil.generateNumber(1, 1000);
                int skuId = RandomUtil.generateNumber(1, 1000);
                int paymentType = RandomUtil.generateNumber(1, 3);
                String tradeNo = UUID.randomUUID().toString();
                double totalAmount = RandomUtil.generateDouble(50, 1000);
                String subject = "退款" + (i + 1);
                int refundStatus = RandomUtil.generateNumber(1, 3);
                LocalDateTime now = LocalDateTime.now();
                LocalDateTime callbackTime = now.plusMinutes(RandomUtil.generateNumber(2, 30));
                
                params.add(new Object[]{
                    id, outTradeNo, orderId, skuId, paymentType, tradeNo, totalAmount,
                    subject, refundStatus, now, callbackTime
                });
            }
            
            dbUtil.batchInsert(sql, params);
            remainingCount -= batchCount;
        }
    }
} 