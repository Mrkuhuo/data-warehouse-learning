package org.bigdatatechcir.warehouse.datageneration.business_code;

import org.bigdatatechcir.warehouse.datageneration.business_code.generator.*;
import org.bigdatatechcir.warehouse.datageneration.business_code.util.DbUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
    
    @Autowired
    private DbUtil dbUtil;
    
    @Autowired
    private BaseDataGenerator baseDataGenerator;
    
    @Autowired
    private ProductDataGenerator productDataGenerator;
    
    @Autowired
    private ActivityDataGenerator activityDataGenerator;
    
    @Autowired
    private CouponDataGenerator couponDataGenerator;
    
    @Autowired
    private OrderDataGenerator orderDataGenerator;
    
    @Autowired
    private UserBehaviorGenerator userBehaviorGenerator;
    
    @Autowired
    private WarehouseDataGenerator warehouseDataGenerator;
    
    @Autowired
    private CMSDataGenerator cmsDataGenerator;
    
    @Autowired
    private UserDataGenerator userDataGenerator;
    
    public static void main(String[] args) {
        SpringApplication.run(BusinessApplication.class, args);
    }
    
    @Override
    public void run(String... args) throws Exception {
        try {
            baseDataGenerator.generateBaseData(batchSize);
            
            while (true) {
                userDataGenerator.generateUserData(batchSize / 10);
                productDataGenerator.generateProductData(batchSize / 10, batchSize / 20);
                activityDataGenerator.generateActivityData(batchSize / 10, batchSize / 20);
                couponDataGenerator.generateCouponData(batchSize / 10, batchSize / 20);
                orderDataGenerator.generateOrderData(batchSize);
                userBehaviorGenerator.generateUserBehaviorData(batchSize);
                warehouseDataGenerator.generateWarehouseData(batchSize / 5);
                cmsDataGenerator.generateCMSData(batchSize / 20, batchSize / 40, batchSize / 100);
                Thread.sleep(interval);
            }
        } catch (Exception e) {
            logger.error("Error generating data", e);
        } finally {
            dbUtil.close();
        }
    }
} 