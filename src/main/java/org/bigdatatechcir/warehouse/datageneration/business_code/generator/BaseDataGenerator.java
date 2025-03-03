package org.bigdatatechcir.warehouse.datageneration.business_code.generator;

import org.bigdatatechcir.warehouse.datageneration.business_code.util.DbUtil;
import org.bigdatatechcir.warehouse.datageneration.business_code.util.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.time.LocalDateTime;

public class BaseDataGenerator {
    private static final Logger logger = LoggerFactory.getLogger(BaseDataGenerator.class);

    public static void generateBaseData(int batchSize) {
        generateBaseCategory(batchSize);
        generateBaseTrademark(batchSize);
        generateBaseAttrInfo(batchSize);
        generateBaseAttrValue(batchSize);
        generateBaseSaleAttr(batchSize);
        generateBaseProvince(batchSize);
        generateBaseRegion(batchSize);
        generateBaseDic(batchSize);
        generateBaseFrontendParam(batchSize);
    }

    private static void generateBaseCategory(int batchSize) {
        // 生成一级分类
        String sql1 = "INSERT INTO base_category1 (id, name) VALUES (?, ?)";
        List<Object[]> params1 = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            params1.add(new Object[]{i, "一级分类" + i});
        }
        DbUtil.batchInsert(sql1, params1);

        // 生成二级分类
        String sql2 = "INSERT INTO base_category2 (id, name, category1_id) VALUES (?, ?, ?)";
        List<Object[]> params2 = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            int category1Id = RandomUtil.generateNumber(1, batchSize);
            params2.add(new Object[]{i, "二级分类" + i, category1Id});
        }
        DbUtil.batchInsert(sql2, params2);

        // 生成三级分类
        String sql3 = "INSERT INTO base_category3 (id, name, category2_id) VALUES (?, ?, ?)";
        List<Object[]> params3 = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            int category2Id = RandomUtil.generateNumber(1, batchSize);
            params3.add(new Object[]{i, "三级分类" + i, category2Id});
        }
        DbUtil.batchInsert(sql3, params3);
    }

    private static void generateBaseTrademark(int batchSize) {
        String sql = "INSERT INTO base_trademark (id, tm_name, logo_url) VALUES (?, ?, ?)";
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            String brand = RandomUtil.generateBrand();
            String logoUrl = "http://example.com/logos/" + brand.toLowerCase() + ".png";
            params.add(new Object[]{i, brand, logoUrl});
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateBaseAttrInfo(int batchSize) {
        String sql = "INSERT INTO base_attr_info (id, attr_name, category_id, category_level) VALUES (?, ?, ?, ?)";
        List<Object[]> params = new ArrayList<>();
        String[] attrNames = {"颜色", "尺码", "材质", "风格", "季节", "适用人群"};
        
        for (int i = 1; i <= batchSize; i++) {
            int categoryId = RandomUtil.generateNumber(1, batchSize);
            int categoryLevel = RandomUtil.generateNumber(1, 3);
            String attrName = attrNames[i % attrNames.length];
            params.add(new Object[]{i, attrName, categoryId, categoryLevel});
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateBaseAttrValue(int batchSize) {
        String sql = "INSERT INTO base_attr_value (id, value_name, attr_id) VALUES (?, ?, ?)";
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            int attrId = RandomUtil.generateNumber(1, batchSize);
            String valueName = RandomUtil.generateColor(); // 使用颜色作为示例属性值
            params.add(new Object[]{i, valueName, attrId});
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateBaseSaleAttr(int batchSize) {
        String sql = "INSERT INTO base_sale_attr (id, name) VALUES (?, ?)";
        List<Object[]> params = new ArrayList<>();
        String[] saleAttrs = {"颜色", "尺码", "版本", "套装", "类型"};
        
        for (int i = 1; i <= Math.min(batchSize, saleAttrs.length); i++) {
            params.add(new Object[]{i, saleAttrs[i-1]});
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateBaseProvince(int batchSize) {
        String sql = "INSERT INTO base_province (id, name, region_id, area_code, iso_code) VALUES (?, ?, ?, ?, ?)";
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            int regionId = RandomUtil.generateNumber(1, 7);
            String areaCode = String.format("%06d", i);
            String isoCode = "CN-" + String.format("%03d", i);
            params.add(new Object[]{i, RandomUtil.PROVINCES[i % RandomUtil.PROVINCES.length], 
                                  regionId, areaCode, isoCode});
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateBaseRegion(int batchSize) {
        String sql = "INSERT INTO base_region (id, region_name) VALUES (?, ?)";
        List<Object[]> params = new ArrayList<>();
        String[] regions = {"华北", "华东", "华南", "华中", "西南", "西北", "东北"};
        
        for (int i = 1; i <= Math.min(batchSize, regions.length); i++) {
            params.add(new Object[]{i, regions[i-1]});
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateBaseDic(int batchSize) {
        String sql = "INSERT INTO base_dic (dic_code, dic_name, parent_code, create_time, operate_time) " +
                    "VALUES (?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        String[][] dicData = {
            {"1001", "年龄段", "0"},
            {"1001001", "0-15岁", "1001"},
            {"1001002", "16-25岁", "1001"},
            {"1001003", "26-35岁", "1001"},
            {"1002", "性别", "0"},
            {"1002001", "男", "1002"},
            {"1002002", "女", "1002"},
            {"1003", "支付方式", "0"},
            {"1003001", "支付宝", "1003"},
            {"1003002", "微信", "1003"},
            {"1003003", "银联", "1003"}
        };
        
        LocalDateTime now = LocalDateTime.now();
        for (String[] dic : dicData) {
            params.add(new Object[]{
                dic[0], dic[1], dic[2], now, now
            });
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateBaseFrontendParam(int batchSize) {
        String sql = "INSERT INTO base_frontend_param (param_name, param_code) VALUES (?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        String[][] paramData = {
            {"颜色", "color"},
            {"尺码", "size"},
            {"风格", "style"},
            {"季节", "season"},
            {"材质", "material"}
        };
        
        for (String[] param : paramData) {
            params.add(new Object[]{param[0], param[1]});
        }
        DbUtil.batchInsert(sql, params);
    }
} 