package org.bigdatatechcir.warehouse.datageneration.business_code.generator;

import org.bigdatatechcir.warehouse.datageneration.business_code.util.DbUtil;
import org.bigdatatechcir.warehouse.datageneration.business_code.util.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class CategoryDataGenerator {
    private static final Logger logger = LoggerFactory.getLogger(CategoryDataGenerator.class);

    @Autowired
    private DbUtil dbUtil;

    public void generateCategoryData(int count) {
        generateCategory3(count);
        generateAttrValue(count);
    }

    private void generateCategory3(int count) {
        // 获取最大ID
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM base_category3";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO base_category3 (id, name, category2_id) VALUES (?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            int category2Id = RandomUtil.generateNumber(101, 200);
            params.add(new Object[]{
                id, "三级分类" + id, category2Id
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateAttrValue(int count) {
        // 获取最大ID
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM base_attr_value";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO base_attr_value (id, value_name, attr_id) VALUES (?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int attrId = (i / 5) + 1; // 每个属性5个值
            String valueName = generateRandomAttrValue(attrId);
            params.add(new Object[]{
                startId + i, valueName, attrId
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private String generateRandomAttrValue(int attrId) {
        String[][] attrValues = {
            {"红色", "蓝色", "黑色", "白色", "灰色"},
            {"S", "M", "L", "XL", "XXL"},
            {"棉", "涤纶", "羊毛", "真丝", "混纺"},
            {"休闲", "正装", "运动", "商务", "时尚"},
            {"春季", "夏季", "秋季", "冬季", "四季"},
            {"男士", "女士", "儿童", "青年", "中老年"}
        };
        
        int valueIndex = RandomUtil.generateNumber(0, 4);
        return attrValues[(attrId - 1) % 6][valueIndex];
    }
} 