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
public class CouponDataGenerator {
    private static final Logger logger = LoggerFactory.getLogger(CouponDataGenerator.class);

    @Autowired
    private DbUtil dbUtil;

    public void generateCouponData(int couponCount, int userCount) {
        logger.info("开始生成优惠券数据...");
        
        logger.info("正在更新 coupon_info 表...");
        generateCouponInfo(couponCount);
        
        logger.info("正在更新 coupon_range 表...");
        generateCouponRange(couponCount);
        
        logger.info("正在更新 coupon_use 表...");
        generateCouponUse(userCount);
        
        logger.info("优惠券数据生成完成");
    }

    private void generateCouponInfo(int count) {
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM coupon_info";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO coupon_info (id, coupon_name, coupon_type, condition_amount, " +
                    "condition_num, activity_id, benefit_amount, benefit_discount, create_time, " +
                    "range_type, limit_num, operate_time, expire_time, range_desc) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();
        
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            String couponType = "CASH";  // 现金券
            BigDecimal conditionAmount = RandomUtil.generatePrice(100, 1000);  // 满100-1000可用
            int conditionNum = RandomUtil.generateNumber(1, 5);  // 需要购买1-5件
            int activityId = RandomUtil.generateNumber(1, 10);  // 关联活动ID
            BigDecimal benefitAmount = RandomUtil.generatePrice(10, 200);  // 优惠10-200元
            BigDecimal benefitDiscount = BigDecimal.ZERO;  // 现金券无折扣
            String rangeType = "CATEGORY";  // 按品类使用
            int limitNum = RandomUtil.generateNumber(1, 5);  // 每人限领1-5张
            LocalDateTime expireTime = now.plusDays(30);  // 30天有效期
            
            params.add(new Object[]{
                id,
                "优惠券" + id,
                couponType,
                conditionAmount,
                conditionNum,
                activityId,
                benefitAmount,
                benefitDiscount,
                now,
                rangeType,
                limitNum,
                now,
                expireTime,
                "指定品类商品"
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateCouponRange(int count) {
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM coupon_range";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO coupon_range (id, coupon_id, range_type, range_id) " +
                    "VALUES (?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            params.add(new Object[]{
                id,
                RandomUtil.generateNumber(1, 10),  // 关联已存在的优惠券
                "CATEGORY",  // 按品类使用
                RandomUtil.generateNumber(1, 10)  // 随机关联一个品类
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateCouponUse(int count) {
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM coupon_use";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO coupon_use (id, coupon_id, user_id, order_id, coupon_status, " +
                    "get_time, using_time, used_time, expire_time) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();
        
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            int couponId = RandomUtil.generateNumber(1, 10);  // 随机优惠券
            int userId = RandomUtil.generateNumber(1, 1000);  // 随机用户
            int orderId = RandomUtil.generateNumber(1, 1000);  // 随机订单
            
            // 优惠券状态：NOT_USED-未使用，USING-使用中，USED-已使用，EXPIRED-已过期
            String[] statuses = {"NOT_USED", "USING", "USED", "EXPIRED"};
            String status = statuses[RandomUtil.generateNumber(0, 3)];
            
            LocalDateTime getTime = now.minusDays(RandomUtil.generateNumber(1, 30));
            LocalDateTime usingTime = null;
            LocalDateTime usedTime = null;
            LocalDateTime expireTime = getTime.plusDays(30);
            
            if ("USING".equals(status) || "USED".equals(status)) {
                usingTime = getTime.plusDays(RandomUtil.generateNumber(1, 7));
                if ("USED".equals(status)) {
                    usedTime = usingTime.plusMinutes(RandomUtil.generateNumber(1, 60));
                }
            }
            
            params.add(new Object[]{
                id,
                couponId,
                userId,
                orderId,
                status,
                getTime,
                usingTime,
                usedTime,
                expireTime
            });
        }
        dbUtil.batchInsert(sql, params);
    }
} 