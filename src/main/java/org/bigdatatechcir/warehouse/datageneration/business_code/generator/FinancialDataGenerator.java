package org.bigdatatechcir.warehouse.datageneration.business_code.generator;

import org.bigdatatechcir.warehouse.datageneration.business_code.util.DbUtil;
import org.bigdatatechcir.warehouse.datageneration.business_code.util.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@Component
public class FinancialDataGenerator {
    private static final Logger logger = LoggerFactory.getLogger(FinancialDataGenerator.class);
    private static final int BATCH_SIZE = 500;

    @Autowired
    private DbUtil dbUtil;

    public void generateFinancialData(int count) {
        generateSkuCost(count);
    }

    private void generateSkuCost(int count) {
        String sql = "INSERT INTO financial_sku_cost (id, sku_id, sku_name, sku_cost, busi_date, is_lastest, create_time) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        int remainingCount = count;
        while (remainingCount > 0) {
            // 获取最大ID
            String maxIdSql = "SELECT COALESCE(MAX(CAST(id AS SIGNED)), 0) FROM financial_sku_cost";
            int startId = dbUtil.queryForInt(maxIdSql) + 1;
            
            int batchCount = Math.min(remainingCount, BATCH_SIZE);
            List<Object[]> params = new ArrayList<>();
            
            for (int i = 0; i < batchCount; i++) {
                String id = String.valueOf(startId + i);
                int skuId = RandomUtil.generateNumber(1, 1000);
                String skuName = generateRandomSkuName();
                double skuCost = RandomUtil.generateDouble(50, 500);
                LocalDateTime now = LocalDateTime.now();
                String busiDate = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                String isLastest = "1";
                
                params.add(new Object[]{
                    id, skuId, skuName, skuCost, busiDate, isLastest, now
                });
            }
            
            dbUtil.batchInsert(sql, params);
            remainingCount -= batchCount;
        }
    }

    private String generateRandomSkuName() {
        String[] styles = {"简约", "潮流", "经典", "时尚", "优雅", "复古", "现代", "自然", "奢华", "清新"};
        String[] types = {"T恤", "裤子", "外套", "连衣裙", "衬衫", "鞋子", "包包", "帽子", "围巾", "手套"};
        
        String style = styles[RandomUtil.generateNumber(0, styles.length - 1)];
        String type = types[RandomUtil.generateNumber(0, types.length - 1)];
        
        return style + type;
    }
} 