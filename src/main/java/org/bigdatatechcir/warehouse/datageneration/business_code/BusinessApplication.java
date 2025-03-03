package org.bigdatatechcir.warehouse.datageneration.business_code;

import org.bigdatatechcir.warehouse.datageneration.business_code.generator.*;
import org.bigdatatechcir.warehouse.datageneration.business_code.util.DbUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.beans.factory.annotation.Value;

@SpringBootApplication
public class BusinessApplication implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(BusinessApplication.class);
    
    @Value("${generator.batch-size:1000}")
    private int batchSize;
    
    @Value("${generator.interval:5000}")
    private long interval;
    
    public static void main(String[] args) {
        SpringApplication.run(BusinessApplication.class, args);
    }
    
    @Override
    public void run(String... args) throws Exception {
        try {
            // 初始化基础数据（只执行一次）
            logger.info("Generating base data...");
            BaseDataGenerator.generateBaseData(batchSize);
            
            // 持续生成业务数据
            logger.info("Start generating business data...");
            while (true) {
                // 生成商品相关数据
                ProductDataGenerator.generateProductData(batchSize / 10);
                
                // 生成活动相关数据
                ActivityDataGenerator.generateActivityData(batchSize / 10);
                
                // 生成优惠券相关数据
                CouponDataGenerator.generateCouponData(batchSize / 10);
                
                // 生成订单相关数据
                OrderDataGenerator.generateOrderData(batchSize);
                
                // 生成用户行为数据
                UserBehaviorGenerator.generateUserBehavior(batchSize);
                
                // 生成仓库相关数据
                WarehouseDataGenerator.generateWarehouseData(batchSize / 5);
                
                // 生成CMS相关数据
                CMSDataGenerator.generateCMSData(batchSize / 20);
                
                logger.info("Generated one batch of data");
                Thread.sleep(interval);
            }
        } catch (Exception e) {
            logger.error("Error generating data", e);
        } finally {
            DbUtil.close();
        }
    }
} 