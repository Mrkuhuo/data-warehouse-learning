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
import java.util.UUID;

@Component
public class PaymentGenerator {
    private static final Logger logger = LoggerFactory.getLogger(PaymentGenerator.class);
    private static final int BATCH_SIZE = 500;

    @Autowired
    private DbUtil dbUtil;

    public void generatePaymentData(int count) {
        String sql = "INSERT INTO payment_info (id, out_trade_no, order_id, user_id, payment_type, " +
                    "trade_no, total_amount, subject, payment_status, create_time, callback_time, " +
                    "callback_content) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        int remainingCount = count;
        while (remainingCount > 0) {
            // 获取最大ID
            String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM payment_info";
            int startId = dbUtil.queryForInt(maxIdSql) + 1;
            
            int batchCount = Math.min(remainingCount, BATCH_SIZE);
            List<Object[]> params = new ArrayList<>();
            
            for (int i = 0; i < batchCount; i++) {
                int id = startId + i;
                int orderId = RandomUtil.generateNumber(1, 1000);
                String outTradeNo = orderId + "";
                int userId = RandomUtil.generateNumber(1, 1000);
                int paymentType = RandomUtil.generateNumber(1, 3);
                String tradeNo = UUID.randomUUID().toString();
                double totalAmount = RandomUtil.generateDouble(100, 10000);
                String subject = "订单支付" + id;
                int paymentStatus = RandomUtil.generateNumber(1, 3);
                LocalDateTime now = LocalDateTime.now();
                LocalDateTime callbackTime = now.plusMinutes(RandomUtil.generateNumber(2, 30));
                String callbackContent = "{\"trade_no\":\"" + tradeNo + "\",\"status\":\"success\"}";
                
                params.add(new Object[]{
                    id, outTradeNo, orderId, userId, paymentType, tradeNo, totalAmount, subject,
                    paymentStatus, now, callbackTime, callbackContent
                });
            }
            
            dbUtil.batchInsert(sql, params);
            remainingCount -= batchCount;
        }
    }
} 