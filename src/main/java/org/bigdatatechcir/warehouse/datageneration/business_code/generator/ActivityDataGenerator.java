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
public class ActivityDataGenerator {
    private static final Logger logger = LoggerFactory.getLogger(ActivityDataGenerator.class);
    
    @Autowired
    private DbUtil dbUtil;

    public void generateActivityData(int activityCount, int skuCount) {
        logger.info("开始生成活动数据...");
        
        logger.info("正在更新 activity_info 表...");
        generateActivityInfo(activityCount);
        
        logger.info("正在更新 activity_rule 表...");
        generateActivityRule(activityCount);
        
        logger.info("正在更新 activity_sku 表...");
        generateActivitySku(skuCount);
        
        logger.info("正在更新 seckill_goods 表...");
        generateSeckillGoods(skuCount);
        
        logger.info("活动数据生成完成");
    }

    private void generateActivityInfo(int count) {
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM activity_info";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO activity_info (id, activity_name, activity_type, activity_desc, " +
                    "start_time, end_time, create_time) VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            LocalDateTime now = LocalDateTime.now();
            LocalDateTime startTime = now.plusDays(RandomUtil.generateNumber(5, 30));
            LocalDateTime endTime = startTime.plusDays(RandomUtil.generateNumber(3, 7));
            
            params.add(new Object[]{
                id,
                "活动" + id,
                RandomUtil.generateNumber(1, 3),  // 活动类型：1-满减，2-折扣，3-秒杀
                "活动描述" + id,
                startTime,
                endTime,
                now
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateActivityRule(int count) {
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM activity_rule";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO activity_rule (id, activity_id, condition_amount, condition_num, " +
                    "benefit_amount, benefit_discount, benefit_level) VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            BigDecimal conditionAmount = RandomUtil.generatePrice(100, 1000);
            int conditionNum = RandomUtil.generateNumber(1, 5);
            BigDecimal benefitAmount = RandomUtil.generatePrice(10, 200);
            BigDecimal benefitDiscount = new BigDecimal("0." + RandomUtil.generateNumber(5, 9));
            int benefitLevel = RandomUtil.generateNumber(1, 3);
            
            params.add(new Object[]{
                id,
                RandomUtil.generateNumber(1, 10),  // 关联已存在的活动
                conditionAmount,
                conditionNum,
                benefitAmount,
                benefitDiscount,
                benefitLevel
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateActivitySku(int count) {
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM activity_sku";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO activity_sku (id, activity_id, sku_id, create_time) VALUES (?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            params.add(new Object[]{
                id,
                RandomUtil.generateNumber(1, 10),  // 关联已存在的活动
                RandomUtil.generateNumber(1, 100),  // 关联已存在的SKU
                LocalDateTime.now()
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateSeckillGoods(int count) {
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM seckill_goods";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO seckill_goods (id, spu_id, sku_id, sku_name, sku_desc, price, " +
                    "cost_price, create_time, check_time, status, start_time, end_time, num, " +
                    "stock_count, sku_default_img) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            LocalDateTime now = LocalDateTime.now();
            LocalDateTime checkTime = now.plusHours(RandomUtil.generateNumber(5, 24));
            LocalDateTime startTime = checkTime.plusHours(5);
            LocalDateTime endTime = startTime.plusHours(2);
            
            BigDecimal price = RandomUtil.generatePrice(100, 1000);
            BigDecimal costPrice = price.multiply(new BigDecimal("0.7"));  // 成本价为售价的70%
            int num = RandomUtil.generateNumber(100, 1000);
            
            params.add(new Object[]{
                id,
                RandomUtil.generateNumber(1, 100),  // 关联已存在的SPU
                RandomUtil.generateNumber(1, 1000),  // 关联已存在的SKU
                "秒杀商品" + id,
                "秒杀商品" + id + "的详细描述",
                price,
                costPrice,
                now,
                checkTime,
                1,  // 状态：1-待审核
                startTime,
                endTime,
                num,
                num,  // 初始库存等于总数量
                "http://example.com/seckill/" + id + ".jpg"
            });
        }
        dbUtil.batchInsert(sql, params);
    }
} 