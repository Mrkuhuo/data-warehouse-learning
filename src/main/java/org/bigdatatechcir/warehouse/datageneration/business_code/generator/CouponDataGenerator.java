package org.bigdatatechcir.warehouse.datageneration.business_code.generator;

import org.bigdatatechcir.warehouse.datageneration.business_code.util.DbUtil;
import org.bigdatatechcir.warehouse.datageneration.business_code.util.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class CouponDataGenerator {
    private static final Logger logger = LoggerFactory.getLogger(CouponDataGenerator.class);

    public static void generateCouponData(int batchSize) {
        generateCouponInfo(batchSize);
        generateCouponRange(batchSize);
        generateCouponUse(batchSize);
    }

    private static void generateCouponInfo(int batchSize) {
        String sql = "INSERT INTO coupon_info (id, coupon_name, coupon_type, condition_amount, condition_num, " +
                    "activity_id, benefit_amount, benefit_discount, create_time, range_type, limit_num, " +
                    "taken_count, start_time, end_time, operate_time, expire_time, range_desc) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            String couponName = "优惠券" + i;
            String couponType = String.valueOf(RandomUtil.generateNumber(1, 3));
            BigDecimal conditionAmount = RandomUtil.generatePrice(100, 1000);
            int conditionNum = RandomUtil.generateNumber(1, 10);
            int activityId = RandomUtil.generateNumber(1, 100);
            BigDecimal benefitAmount = RandomUtil.generatePrice(10, 200);
            BigDecimal benefitDiscount = new BigDecimal(String.format("%.2f", 0.1 * RandomUtil.generateNumber(5, 9)));
            LocalDateTime now = LocalDateTime.now();
            String rangeType = String.valueOf(RandomUtil.generateNumber(1, 3));
            int limitNum = RandomUtil.generateNumber(1, 5);
            int takenCount = RandomUtil.generateNumber(0, 1000);
            LocalDateTime startTime = now.plusDays(RandomUtil.generateNumber(1, 7));
            LocalDateTime endTime = startTime.plusDays(RandomUtil.generateNumber(1, 30));
            LocalDateTime operateTime = now;
            LocalDateTime expireTime = endTime;
            String rangeDesc = "使用范围描述" + i;

            params.add(new Object[]{
                i, couponName, couponType, conditionAmount, conditionNum, activityId,
                benefitAmount, benefitDiscount, now, rangeType, limitNum, takenCount,
                startTime, endTime, operateTime, expireTime, rangeDesc
            });
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateCouponRange(int batchSize) {
        String sql = "INSERT INTO coupon_range (id, coupon_id, range_type, range_id) VALUES (?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            int couponId = RandomUtil.generateNumber(1, batchSize);
            String rangeType = String.valueOf(RandomUtil.generateNumber(1, 3));
            String rangeId = String.valueOf(RandomUtil.generateNumber(1, 100));

            params.add(new Object[]{i, couponId, rangeType, rangeId});
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateCouponUse(int batchSize) {
        String sql = "INSERT INTO coupon_use (id, coupon_id, user_id, order_id, coupon_status, get_time, " +
                    "using_time, used_time, expire_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            int couponId = RandomUtil.generateNumber(1, batchSize);
            int userId = RandomUtil.generateNumber(1, 1000);
            String orderId = RandomUtil.generateOrderNo();
            String couponStatus = String.valueOf(RandomUtil.generateNumber(1, 3));
            LocalDateTime now = LocalDateTime.now();
            LocalDateTime getTime = now.minusDays(RandomUtil.generateNumber(1, 30));
            LocalDateTime usingTime = getTime.plusDays(RandomUtil.generateNumber(1, 7));
            LocalDateTime usedTime = usingTime.plusMinutes(RandomUtil.generateNumber(1, 60));
            LocalDateTime expireTime = getTime.plusDays(30);

            params.add(new Object[]{
                i, couponId, userId, orderId, couponStatus, getTime, usingTime,
                usedTime, expireTime
            });
        }
        DbUtil.batchInsert(sql, params);
    }
} 