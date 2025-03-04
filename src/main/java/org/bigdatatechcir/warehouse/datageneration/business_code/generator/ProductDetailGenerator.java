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
public class ProductDetailGenerator {
    private static final Logger logger = LoggerFactory.getLogger(ProductDetailGenerator.class);

    @Autowired
    private DbUtil dbUtil;

    public void generateProductDetailData(int skuCount, int spuCount) {
        generateSkuAttrValue(skuCount);
        generateSkuImage(skuCount);
        generateSkuSaleAttrValue(skuCount);
        generateSpuImage(spuCount);
        generateSpuPoster(spuCount);
        generateSpuSaleAttr(spuCount);
        generateSpuSaleAttrValue(spuCount);
    }

    private void generateSkuAttrValue(int count) {
        // 获取最大ID
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM sku_attr_value";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO sku_attr_value (id, attr_id, value_id, sku_id, attr_name, value_name) " +
                    "VALUES (?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            int baseId = startId + (i - 1) * 4; // 每个SKU生成4个属性值，所以ID要间隔4
            
            // 为每个SKU生成4个属性值
            for(int j = 0; j < 4; j++) {
                int id = baseId + j;
                int skuId = RandomUtil.generateNumber(1, 35); // 根据sku_info表的实际数据
                
                int attrId;
                int valueId;
                String attrName;
                String valueName;
                
                switch(j) {
                    case 0: // 手机一级
                        attrId = 106;
                        valueId = RandomUtil.generateNumber(175, 176);
                        attrName = "手机一级";
                        valueName = valueId == 175 ? "苹果手机" : "安卓手机";
                        break;
                    case 1: // 二级手机
                        attrId = 107;
                        valueId = RandomUtil.generateNumber(177, 179);
                        attrName = "二级手机";
                        valueName = valueId == 177 ? "小米" : (valueId == 178 ? "华为" : "苹果");
                        break;
                    case 2: // 运行内存
                        attrId = 23;
                        valueId = RandomUtil.generateNumber(14, 169);
                        attrName = "运行内存";
                        valueName = valueId == 14 ? "4G" : (valueId == 83 ? "8G" : "6G");
                        break;
                    default: // 机身内存
                        attrId = 24;
                        valueId = RandomUtil.generateNumber(82, 166);
                        attrName = "机身内存";
                        valueName = valueId == 82 ? "128G" : "256G";
                        break;
                }
                
                params.add(new Object[]{id, attrId, valueId, skuId, attrName, valueName});
            }
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
        for (int i = 1; i <= count; i++) {
            int id = startId + i;
            int skuId = RandomUtil.generateNumber(1, 35); // 根据sku_info表的实际数据
            String imgName = "商品图片" + i;
            String imgUrl = "http://example.com/images/sku" + skuId + "_" + i + ".jpg";
            int spuImgId = RandomUtil.generateNumber(1, 96); // 根据spu_image表的实际数据
            String isDefault = i == 1 ? "1" : "0";
            
            params.add(new Object[]{id, skuId, imgName, imgUrl, spuImgId, isDefault});
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateSkuSaleAttrValue(int count) {
        // 获取最大ID
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM sku_sale_attr_value";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO sku_sale_attr_value (id, sku_id, spu_id, sale_attr_value_id, " +
                    "sale_attr_id, sale_attr_name, sale_attr_value_name) VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            int baseId = startId + (i - 1) * 2; // 每个SKU生成2个销售属性值，所以ID要间隔2
            int skuId = RandomUtil.generateNumber(1, 35); // 根据sku_info表的实际数据
            int spuId = RandomUtil.generateNumber(1, 12); // 根据spu_info表的实际数据
            
            // 为每个SKU生成2个销售属性值
            for(int j = 0; j < 2; j++) {
                int id = baseId + j;
                int saleAttrId = j == 0 ? 1 : 2; // 1:颜色, 2:版本
                int saleAttrValueId = RandomUtil.generateNumber(1, 20);
                String saleAttrName = saleAttrId == 1 ? "颜色" : "版本";
                String saleAttrValueName;
                
                if(saleAttrId == 1) { // 颜色
                    String[] colors = {"陶瓷黑", "透明版", "冰雾白", "明月灰", "亮黑色", "冰霜银"};
                    saleAttrValueName = colors[RandomUtil.generateNumber(0, colors.length - 1)];
                } else { // 版本
                    String[] versions = {"8G+128G", "16G+256G", "4G+128G", "6G+128G"};
                    saleAttrValueName = versions[RandomUtil.generateNumber(0, versions.length - 1)];
                }
                
                params.add(new Object[]{
                    id, skuId, spuId, saleAttrValueId, saleAttrId,
                    saleAttrName, saleAttrValueName
                });
            }
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateSpuImage(int count) {
        // 获取最大ID
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM spu_image";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO spu_image (id, spu_id, img_name, img_url) VALUES (?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            int id = startId + i;
            int spuId = RandomUtil.generateNumber(1, count);
            String imgName = "SPU图片" + i;
            String imgUrl = "http://example.com/images/spu" + spuId + "_" + i + ".jpg";
            
            params.add(new Object[]{id, spuId, imgName, imgUrl});
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateSpuPoster(int count) {
        // 获取最大ID
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM spu_poster";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO spu_poster (id, spu_id, img_name, img_url, create_time, update_time, is_deleted) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            int id = startId + i;
            int spuId = RandomUtil.generateNumber(1, count);
            String imgName = "SPU海报" + i;
            String imgUrl = "http://example.com/posters/spu" + spuId + "_" + i + ".jpg";
            LocalDateTime now = LocalDateTime.now();
            
            params.add(new Object[]{id, spuId, imgName, imgUrl, now, now, 0});
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateSpuSaleAttr(int count) {
        // 获取最大ID
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM spu_sale_attr";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO spu_sale_attr (id, spu_id, base_sale_attr_id, sale_attr_name) " +
                    "VALUES (?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            int id = startId + i;
            int spuId = RandomUtil.generateNumber(1, count);
            int baseSaleAttrId = RandomUtil.generateNumber(1, 4);
            String saleAttrName = "销售属性" + baseSaleAttrId;
            
            params.add(new Object[]{id, spuId, baseSaleAttrId, saleAttrName});
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateSpuSaleAttrValue(int count) {
        // 获取最大ID
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM spu_sale_attr_value";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO spu_sale_attr_value (id, spu_id, base_sale_attr_id, sale_attr_value_name, " +
                    "sale_attr_name) VALUES (?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            int spuId = RandomUtil.generateNumber(1, 12); // 根据spu_info表的实际数据
            int baseSaleAttrId = RandomUtil.generateNumber(1, 4); // 根据base_sale_attr表的实际数据
            String saleAttrValueName = "销售属性值" + id;
            String saleAttrName = getSaleAttrName(baseSaleAttrId);
            
            params.add(new Object[]{id, spuId, baseSaleAttrId, saleAttrValueName, saleAttrName});
        }
        dbUtil.batchInsert(sql, params);
    }

    private String getSaleAttrName(int baseSaleAttrId) {
        switch(baseSaleAttrId) {
            case 1: return "颜色";
            case 2: return "版本";
            case 3: return "尺码";
            case 4: return "类别";
            default: return "未知";
        }
    }
} 