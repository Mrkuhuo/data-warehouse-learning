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
public class ProductDataGenerator {
    private static final Logger logger = LoggerFactory.getLogger(ProductDataGenerator.class);

    @Autowired
    private DbUtil dbUtil;

    public void generateProductData(int spuCount, int skuCount) {
        generateSpuInfo(spuCount);
        generateSkuInfo(skuCount);
        generateSkuImage(skuCount);
        generateSkuAttrValue(skuCount);
        generateSkuSaleAttrValue(skuCount);
    }

    private void generateSpuInfo(int count) {
        // 获取最大ID
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM spu_info";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO spu_info (id, spu_name, description, category3_id, tm_id) " +
                    "VALUES (?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            String spuName = RandomUtil.generateProductName();
            String description = "商品" + id + "的详细描述";
            int category3Id = RandomUtil.generateNumber(1, 1099); // 根据base_category3表的实际数据
            int tmId = RandomUtil.generateNumber(1, 50); // 根据base_trademark表的实际数据
            
            params.add(new Object[]{
                id, spuName, description, category3Id, tmId
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateSkuInfo(int count) {
        // 获取最大ID
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM sku_info";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO sku_info (id, spu_id, price, sku_name, sku_desc, weight, tm_id, " +
                    "category3_id, sku_default_img, is_sale) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            int spuId = RandomUtil.generateNumber(1, 12); // 根据spu_info表的实际数据
            BigDecimal price = RandomUtil.generatePrice(10, 1000);
            String skuName = "SKU" + id;
            String skuDesc = "SKU" + id + "的详细描述";
            BigDecimal weight = RandomUtil.generatePrice(0.1, 10.0);
            int tmId = RandomUtil.generateNumber(1, 50); // 根据base_trademark表的实际数据
            int category3Id = RandomUtil.generateNumber(1, 1099); // 根据base_category3表的实际数据
            String skuDefaultImg = "http://example.com/images/sku" + id + ".jpg";
            int isSale = 1;
            
            params.add(new Object[]{
                id, spuId, price, skuName, skuDesc, weight, tmId,
                category3Id, skuDefaultImg, isSale
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateSkuImage(int count) {
        // 获取最大ID
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM sku_image";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO sku_image (id, sku_id, img_name, img_url, spu_img_id, is_default) " +
                    "VALUES (?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            int skuId = RandomUtil.generateNumber(1, 35); // 根据sku_info表的实际数据
            String imgName = "图片" + id;
            String imgUrl = "http://example.com/images/sku" + id + ".jpg";
            int spuImgId = RandomUtil.generateNumber(1, 12); // 根据spu_image表的实际数据
            int isDefault = i % 5 == 0 ? 1 : 0; // 每5个图片设置一个默认图片
            
            params.add(new Object[]{
                id, skuId, imgName, imgUrl, spuImgId, isDefault
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateSkuAttrValue(int count) {
        // 获取最大ID
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM sku_attr_value";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO sku_attr_value (id, attr_id, value_id, sku_id, attr_name, " +
                    "value_name) VALUES (?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            int attrId = RandomUtil.generateNumber(1, 114); // 根据base_attr_info表的实际数据
            int valueId = RandomUtil.generateNumber(1, 218); // 根据base_attr_value表的实际数据
            int skuId = RandomUtil.generateNumber(1, 35); // 根据sku_info表的实际数据
            String attrName = "属性" + attrId;
            String valueName = "属性值" + valueId;
            
            params.add(new Object[]{
                id, attrId, valueId, skuId, attrName, valueName
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateSkuSaleAttrValue(int count) {
        // 获取最大ID
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM sku_sale_attr_value";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO sku_sale_attr_value (id, sku_id, spu_id, sale_attr_value_id, " +
                    "sale_attr_id, sale_attr_name, sale_attr_value_name) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            int skuId = RandomUtil.generateNumber(1, 35); // 根据sku_info表的实际数据
            int spuId = RandomUtil.generateNumber(1, 12); // 根据spu_info表的实际数据
            int saleAttrValueId = RandomUtil.generateNumber(1, 100);
            int saleAttrId = RandomUtil.generateNumber(1, 4); // 根据base_sale_attr表的实际数据
            String saleAttrName = "销售属性" + saleAttrId;
            String saleAttrValueName = "销售属性值" + saleAttrValueId;
            
            params.add(new Object[]{
                id, skuId, spuId, saleAttrValueId, saleAttrId,
                saleAttrName, saleAttrValueName
            });
        }
        dbUtil.batchInsert(sql, params);
    }
} 