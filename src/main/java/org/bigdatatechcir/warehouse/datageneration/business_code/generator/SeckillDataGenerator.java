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
public class SeckillDataGenerator {
    private static final Logger logger = LoggerFactory.getLogger(SeckillDataGenerator.class);

    @Autowired
    private DbUtil dbUtil;

    public void generateSeckillData(int count) {
        // 获取最大ID
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM seckill_goods";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO seckill_goods (id, spu_id, sku_id, sku_name, sku_default_img, " +
                    "price, cost_price, create_time, check_time, status, start_time, end_time, " +
                    "num, stock_count, sku_desc) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            int spuId = RandomUtil.generateNumber(1, 12); // 从已有SPU中选择
            int skuId = RandomUtil.generateNumber(1, 35); // 从已有SKU中选择
            String skuName = "秒杀商品" + id;
            String skuDefaultImg = "http://example.com/seckill/" + id + ".jpg";
            BigDecimal price = RandomUtil.generatePrice(100, 1000);
            BigDecimal costPrice = price.multiply(new BigDecimal("0.7")); // 秒杀价格为原价7折
            
            LocalDateTime now = LocalDateTime.now();
            LocalDateTime checkTime = now.plusHours(RandomUtil.generateNumber(1, 24));
            String status = String.valueOf(RandomUtil.generateNumber(0, 1));
            LocalDateTime startTime = now.plusDays(1);
            LocalDateTime endTime = startTime.plusHours(2); // 秒杀持续2小时
            
            int num = RandomUtil.generateNumber(100, 1000);
            int stockCount = num; // 初始库存等于总数
            String skuDesc = "秒杀商品" + id + "的详细描述";
            
            params.add(new Object[]{
                id, spuId, skuId, skuName, skuDefaultImg,
                price, costPrice, now, checkTime, status,
                startTime, endTime, num, stockCount, skuDesc
            });
        }
        dbUtil.batchInsert(sql, params);
    }
} 