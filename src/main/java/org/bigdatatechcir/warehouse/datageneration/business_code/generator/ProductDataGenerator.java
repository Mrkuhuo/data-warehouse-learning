package org.bigdatatechcir.warehouse.datageneration.business_code.generator;

import org.bigdatatechcir.warehouse.datageneration.business_code.util.DbUtil;
import org.bigdatatechcir.warehouse.datageneration.business_code.util.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class ProductDataGenerator {
    private static final Logger logger = LoggerFactory.getLogger(ProductDataGenerator.class);

    public static void generateProductData(int batchSize) {
        generateSkuInfo(batchSize);
        generateSkuImage(batchSize);
        generateSkuAttrValue(batchSize);
        generateSkuSaleAttrValue(batchSize);
        generateSpuInfo(batchSize);
        generateSpuImage(batchSize);
        generateSpuPoster(batchSize);
        generateSpuSaleAttr(batchSize);
        generateSpuSaleAttrValue(batchSize);
        generateFinancialSkuCost(batchSize);
    }

    private static void generateSkuInfo(int batchSize) {
        String sql = "INSERT INTO sku_info (id, spu_id, price, sku_name, sku_desc, weight, tm_id, category3_id, " +
                    "sku_default_img, is_sale, create_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            String skuId = RandomUtil.generateSku();
            String spuId = RandomUtil.generateSpuCode();
            BigDecimal price = RandomUtil.generatePrice(100, 1000);
            String skuName = "商品" + i;
            String skuDesc = "商品描述" + i;
            BigDecimal weight = RandomUtil.generatePrice(0.1, 10);
            int tmId = RandomUtil.generateNumber(1, 100);
            int category3Id = RandomUtil.generateNumber(1, 100);
            String skuDefaultImg = "http://example.com/images/" + skuId + ".jpg";
            String isSale = "1";
            LocalDateTime createTime = LocalDateTime.now();

            params.add(new Object[]{
                skuId, spuId, price, skuName, skuDesc, weight, tmId, category3Id,
                skuDefaultImg, isSale, createTime
            });
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateSkuImage(int batchSize) {
        String sql = "INSERT INTO sku_image (id, sku_id, img_name, img_url, spu_img_id, is_default) " +
                    "VALUES (?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            String skuId = RandomUtil.generateSku();
            String imgName = "图片" + i;
            String imgUrl = "http://example.com/images/" + skuId + "/" + i + ".jpg";
            String spuImgId = RandomUtil.generateUUID();
            String isDefault = i == 1 ? "1" : "0";

            params.add(new Object[]{i, skuId, imgName, imgUrl, spuImgId, isDefault});
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateSkuAttrValue(int batchSize) {
        String sql = "INSERT INTO sku_attr_value (id, attr_id, value_id, sku_id, attr_name, value_name) " +
                    "VALUES (?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            int attrId = RandomUtil.generateNumber(1, 100);
            int valueId = RandomUtil.generateNumber(1, 100);
            String skuId = RandomUtil.generateSku();
            String attrName = "属性" + attrId;
            String valueName = RandomUtil.generateColor();

            params.add(new Object[]{i, attrId, valueId, skuId, attrName, valueName});
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateSkuSaleAttrValue(int batchSize) {
        String sql = "INSERT INTO sku_sale_attr_value (id, sku_id, spu_id, sale_attr_value_id, sale_attr_id, " +
                    "sale_attr_name, sale_attr_value_name) VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            String skuId = RandomUtil.generateSku();
            String spuId = RandomUtil.generateSpuCode();
            int saleAttrValueId = RandomUtil.generateNumber(1, 100);
            int saleAttrId = RandomUtil.generateNumber(1, 10);
            String saleAttrName = "销售属性" + saleAttrId;
            String saleAttrValueName = RandomUtil.generateSize();

            params.add(new Object[]{
                i, skuId, spuId, saleAttrValueId, saleAttrId, saleAttrName, saleAttrValueName
            });
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateSpuInfo(int batchSize) {
        String sql = "INSERT INTO spu_info (id, spu_name, description, category3_id, tm_id) " +
                    "VALUES (?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            String spuId = RandomUtil.generateSpuCode();
            String spuName = "商品" + i;
            String description = "商品描述" + i;
            int category3Id = RandomUtil.generateNumber(1, 100);
            int tmId = RandomUtil.generateNumber(1, 100);

            params.add(new Object[]{spuId, spuName, description, category3Id, tmId});
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateSpuImage(int batchSize) {
        String sql = "INSERT INTO spu_image (id, spu_id, img_name, img_url) VALUES (?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            String spuId = RandomUtil.generateSpuCode();
            String imgName = "图片" + i;
            String imgUrl = "http://example.com/images/" + spuId + "/" + i + ".jpg";

            params.add(new Object[]{i, spuId, imgName, imgUrl});
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateSpuPoster(int batchSize) {
        String sql = "INSERT INTO spu_poster (id, spu_id, img_name, img_url) VALUES (?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            String spuId = RandomUtil.generateSpuCode();
            String imgName = "海报" + i;
            String imgUrl = "http://example.com/posters/" + spuId + "/" + i + ".jpg";

            params.add(new Object[]{i, spuId, imgName, imgUrl});
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateSpuSaleAttr(int batchSize) {
        String sql = "INSERT INTO spu_sale_attr (id, spu_id, base_sale_attr_id, sale_attr_name) " +
                    "VALUES (?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            String spuId = RandomUtil.generateSpuCode();
            int baseSaleAttrId = RandomUtil.generateNumber(1, 10);
            String saleAttrName = "销售属性" + baseSaleAttrId;

            params.add(new Object[]{i, spuId, baseSaleAttrId, saleAttrName});
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateSpuSaleAttrValue(int batchSize) {
        String sql = "INSERT INTO spu_sale_attr_value (id, spu_id, base_sale_attr_id, sale_attr_value_name, " +
                    "sale_attr_name) VALUES (?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            String spuId = RandomUtil.generateSpuCode();
            int baseSaleAttrId = RandomUtil.generateNumber(1, 10);
            String saleAttrValueName = RandomUtil.generateSize();
            String saleAttrName = "销售属性" + baseSaleAttrId;

            params.add(new Object[]{i, spuId, baseSaleAttrId, saleAttrValueName, saleAttrName});
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateFinancialSkuCost(int batchSize) {
        String sql = "INSERT INTO financial_sku_cost (id, sku_id, sku_name, busi_date, is_lastest, " +
                    "sku_cost, create_time) VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            String skuId = RandomUtil.generateSku();
            String skuName = "商品" + i;
            LocalDateTime busiDate = LocalDateTime.now();
            String isLastest = "1";
            BigDecimal skuCost = RandomUtil.generatePrice(50, 500);
            LocalDateTime createTime = LocalDateTime.now();

            params.add(new Object[]{
                i, skuId, skuName, busiDate, isLastest, skuCost, createTime
            });
        }
        DbUtil.batchInsert(sql, params);
    }
} 