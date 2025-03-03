package org.bigdatatechcir.warehouse.datageneration.business_code.generator;

import org.bigdatatechcir.warehouse.datageneration.business_code.util.DbUtil;
import org.bigdatatechcir.warehouse.datageneration.business_code.util.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class ActivityDataGenerator {
    private static final Logger logger = LoggerFactory.getLogger(ActivityDataGenerator.class);

    public static void generateActivityData(int batchSize) {
        generateActivityInfo(batchSize);
        generateActivityRule(batchSize);
        generateActivitySku(batchSize);
        generateSeckillGoods(batchSize);
    }

    private static void generateActivityInfo(int batchSize) {
        String sql = "INSERT INTO activity_info (id, activity_name, activity_type, activity_desc, start_time, " +
                    "end_time, create_time) VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            String activityName = "活动" + i;
            String activityType = String.valueOf(RandomUtil.generateNumber(1, 3)); // 1:满减 2:折扣 3:秒杀
            String activityDesc = "活动描述" + i;
            LocalDateTime now = LocalDateTime.now();
            LocalDateTime startTime = now.plusDays(RandomUtil.generateNumber(1, 7));
            LocalDateTime endTime = startTime.plusDays(RandomUtil.generateNumber(1, 30));

            params.add(new Object[]{
                i, activityName, activityType, activityDesc, startTime, endTime, now
            });
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateActivityRule(int batchSize) {
        String sql = "INSERT INTO activity_rule (id, activity_id, condition_amount, condition_num, benefit_amount, " +
                    "benefit_discount, benefit_level) VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            int activityId = RandomUtil.generateNumber(1, batchSize);
            BigDecimal conditionAmount = RandomUtil.generatePrice(100, 1000);
            int conditionNum = RandomUtil.generateNumber(1, 10);
            BigDecimal benefitAmount = RandomUtil.generatePrice(10, 200);
            BigDecimal benefitDiscount = new BigDecimal(String.format("%.2f", 0.1 * RandomUtil.generateNumber(5, 9)));
            int benefitLevel = RandomUtil.generateNumber(1, 3);

            params.add(new Object[]{
                i, activityId, conditionAmount, conditionNum, benefitAmount, benefitDiscount, benefitLevel
            });
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateActivitySku(int batchSize) {
        String sql = "INSERT INTO activity_sku (id, activity_id, sku_id, create_time) VALUES (?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            int activityId = RandomUtil.generateNumber(1, batchSize);
            String skuId = RandomUtil.generateSku();
            LocalDateTime createTime = LocalDateTime.now();

            params.add(new Object[]{i, activityId, skuId, createTime});
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateSeckillGoods(int batchSize) {
        String sql = "INSERT INTO seckill_goods (id, spu_id, sku_id, sku_name, sku_desc, price, cost_price, " +
                    "check_time, status, num, stock_count, sku_default_img) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            String spuId = RandomUtil.generateSpuCode();
            String skuId = RandomUtil.generateSku();
            String skuName = "商品" + i;
            String skuDesc = "商品描述" + i;
            BigDecimal price = RandomUtil.generatePrice(100, 1000);
            BigDecimal costPrice = RandomUtil.generatePrice(50, price.doubleValue());
            LocalDateTime checkTime = LocalDateTime.now();
            String status = "1";
            int num = RandomUtil.generateNumber(100, 1000);
            int stockCount = RandomUtil.generateNumber(0, num);
            String skuDefaultImg = "http://example.com/images/" + skuId + ".jpg";

            params.add(new Object[]{
                i, spuId, skuId, skuName, skuDesc, price, costPrice, checkTime,
                status, num, stockCount, skuDefaultImg
            });
        }
        DbUtil.batchInsert(sql, params);
    }
} 