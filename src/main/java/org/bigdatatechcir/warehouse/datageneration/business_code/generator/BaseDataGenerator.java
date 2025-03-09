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

@Component
public class BaseDataGenerator {
    private static final Logger logger = LoggerFactory.getLogger(BaseDataGenerator.class);

    @Autowired
    private DbUtil dbUtil;

    @Autowired
    private RandomUtil randomUtil;

    public void generateBaseData(int batchSize) {
        generateCategory1(batchSize);
        generateCategory2(batchSize);
        generateCategory3(batchSize);
        generateTrademark(batchSize);
        generateAttrInfo(batchSize);
        generateAttrValue(batchSize);
        generateSaleAttr();
        generateProvince(batchSize);
        generateRegion();
        generateDict();
        generateFrontendParam();
    }

    private void generateCategory1(int batchSize) {
        logger.info("正在更新 base_category1 表...");
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM base_category1";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO base_category1 (id, name) VALUES (?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            int id = startId + i;
            params.add(new Object[]{
                id,
                "一级分类" + id
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateCategory2(int batchSize) {
        logger.info("正在更新 base_category2 表...");
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM base_category2";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO base_category2 (id, name, category1_id) VALUES (?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            int id = startId + i;
            params.add(new Object[]{
                id,
                "二级分类" + id,
                randomUtil.generateNumber(1, 100)  // 关联已存在的一级分类
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateCategory3(int batchSize) {
        logger.info("正在更新 base_category3 表...");
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM base_category3";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO base_category3 (id, name, category2_id) VALUES (?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            int id = startId + i;
            params.add(new Object[]{
                id,
                "三级分类" + id,
                randomUtil.generateNumber(1, 100)  // 关联已存在的二级分类
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateTrademark(int batchSize) {
        logger.info("正在更新 base_trademark 表...");
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM base_trademark";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO base_trademark (id, tm_name) VALUES (?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            int id = startId + i;
            params.add(new Object[]{
                id,
                "品牌" + id
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateAttrInfo(int batchSize) {
        logger.info("正在更新 base_attr_info 表...");
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM base_attr_info";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO base_attr_info (id, attr_name, category_id, category_level) VALUES (?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        String[] attrNames = {"尺码", "颜色", "材质", "风格", "季节", "适用人群"};
        
        for (int i = 0; i < batchSize; i++) {
            int id = startId + i;
            params.add(new Object[]{
                id,
                attrNames[i % attrNames.length],
                randomUtil.generateNumber(1, 100),  // 关联已存在的分类
                randomUtil.generateNumber(1, 3)
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateAttrValue(int batchSize) {
        logger.info("正在更新 base_attr_value 表...");
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM base_attr_value";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO base_attr_value (id, value_name, attr_id) VALUES (?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        String[] colors = {"红色", "蓝色", "黑色", "白色", "金色", "银色", "粉色"};
        
        for (int i = 0; i < batchSize; i++) {
            int id = startId + i;
            params.add(new Object[]{
                id,
                colors[i % colors.length],
                randomUtil.generateNumber(1, 100)  // 关联已存在的属性
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateSaleAttr() {
        logger.info("正在更新 base_sale_attr 表...");
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM base_sale_attr";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO base_sale_attr (id, name) VALUES (?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        String[] saleAttrs = {"颜色", "尺码", "版本", "套装", "类型"};
        
        for (int i = 0; i < saleAttrs.length; i++) {
            int id = startId + i;
            params.add(new Object[]{
                id,
                saleAttrs[i]
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateProvince(int batchSize) {
        logger.info("正在更新 base_province 表...");
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM base_province";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO base_province (id, name, region_id, area_code, iso_code, iso_3166_2) " +
                    "VALUES (?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            int id = startId + i;
            params.add(new Object[]{
                id,
                randomUtil.PROVINCES[i % randomUtil.PROVINCES.length],
                randomUtil.generateNumber(1, 7),  // 关联已存在的地区
                randomUtil.generateNumber(100000, 999999),
                "CN-" + String.format("%02d", id),
                "CN-" + String.format("%02d", id)
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateRegion() {
        logger.info("正在更新 base_region 表...");
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM base_region";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO base_region (id, region_name) VALUES (?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        String[] regions = {"华北", "华东", "华南", "华中", "西南", "西北", "东北"};
        
        for (int i = 0; i < regions.length; i++) {
            int id = startId + i;
            params.add(new Object[]{
                id,
                regions[i]
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateDict() {
        logger.info("正在更新 base_dic 表...");
        String sql = "INSERT INTO base_dic (dic_code, dic_name, parent_code, create_time, operate_time) " +
                    "VALUES (?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();
        
        // 添加父级字典
        params.add(new Object[]{1001, "年龄段", 0, now, now});
        params.add(new Object[]{1002, "性别", 0, now, now});
        params.add(new Object[]{1003, "支付方式", 0, now, now});
        
        // 添加年龄段子项
        params.add(new Object[]{1001001, "0-15岁", 1001, now, now});
        params.add(new Object[]{1001002, "16-25岁", 1001, now, now});
        params.add(new Object[]{1001003, "26-35岁", 1001, now, now});
        
        // 添加性别子项
        params.add(new Object[]{1002001, "男", 1002, now, now});
        params.add(new Object[]{1002002, "女", 1002, now, now});
        
        // 添加支付方式子项
        params.add(new Object[]{1003001, "支付宝", 1003, now, now});
        params.add(new Object[]{1003002, "微信", 1003, now, now});
        params.add(new Object[]{1003003, "银联", 1003, now, now});
        
        dbUtil.batchInsert(sql, params);
    }

    private void generateFrontendParam() {
        logger.info("正在更新 base_frontend_param 表...");
        String sql = "INSERT INTO base_frontend_param (code) VALUES (?)";
        
        List<Object[]> params = new ArrayList<>();
        String[] frontendParams = {
            "color",
            "size",
            "style",
            "season",
            "material"
        };
        
        for (String param : frontendParams) {
            params.add(new Object[]{param});
        }
        dbUtil.batchInsert(sql, params);
    }
} 