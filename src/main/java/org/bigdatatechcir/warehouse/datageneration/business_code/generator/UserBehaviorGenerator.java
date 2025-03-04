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
public class UserBehaviorGenerator {
    private static final Logger logger = LoggerFactory.getLogger(UserBehaviorGenerator.class);
    private static final int BATCH_SIZE = 500;
    private static final int GENERATION_INTERVAL = 1000; // 生成间隔（毫秒）

    @Autowired
    private DbUtil dbUtil;

    public void generateUserBehaviorData(int count) {
        generateCartInfo(count);
        generateCommentInfo(count);
        generateFavorInfo(count);
    }

    private void generateCartInfo(int count) {
        logger.info("正在更新 cart_info 表...");
        String sql = "INSERT INTO cart_info (id, user_id, sku_id, cart_price, sku_num, " +
                    "img_url, sku_name, is_checked, create_time, operate_time, is_ordered, " +
                    "order_time, source_type, source_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM cart_info";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        List<Object[]> params = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();
        
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            int userId = RandomUtil.generateNumber(1, 1000);
            int skuId = RandomUtil.generateNumber(1, 1000);
            BigDecimal cartPrice = RandomUtil.generatePrice(10, 1000);
            int skuNum = RandomUtil.generateNumber(1, 5);
            String imgUrl = "http://example.com/products/" + skuId + ".jpg";
            String skuName = "商品" + skuId;
            
            params.add(new Object[]{
                id,
                userId,
                skuId,
                cartPrice,
                skuNum,
                imgUrl,
                skuName,
                RandomUtil.generateNumber(0, 1),  // is_checked
                now,
                now,
                0,  // is_ordered
                null,  // order_time
                "2401",  // source_type: 用户查询
                null  // source_id
            });
        }
        
        dbUtil.batchInsert(sql, params);
    }

    private void generateCommentInfo(int count) {
        logger.info("正在更新 comment_info 表...");
        String sql = "INSERT INTO comment_info (id, user_id, nick_name, head_img, sku_id, " +
                    "spu_id, order_id, appraise, comment_txt, create_time, operate_time) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM comment_info";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        List<Object[]> params = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();
        
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            int userId = RandomUtil.generateNumber(1, 1000);
            String nickName = "用户" + userId;
            String headImg = "http://example.com/avatars/" + userId + ".jpg";
            int skuId = RandomUtil.generateNumber(1, 1000);
            int spuId = RandomUtil.generateNumber(1, 100);
            int orderId = RandomUtil.generateNumber(1000000, 9999999);
            String appraise = String.valueOf(RandomUtil.generateNumber(1, 3)); // 1好评 2中评 3差评
            String commentTxt = "商品" + skuId + "的评价内容";
            
            params.add(new Object[]{
                id, userId, nickName, headImg, skuId,
                spuId, orderId, appraise, commentTxt, now, now
            });
        }
        
        dbUtil.batchInsert(sql, params);
    }

    private void generateFavorInfo(int count) {
        logger.info("正在更新 favor_info 表...");
        String sql = "INSERT INTO favor_info (id, user_id, sku_id, spu_id, is_cancel, " +
                    "create_time, cancel_time) VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM favor_info";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        List<Object[]> params = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();
        
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            int userId = RandomUtil.generateNumber(1, 1000);
            int skuId = RandomUtil.generateNumber(1, 1000);
            int spuId = RandomUtil.generateNumber(1, 100);
            String isCancel = String.valueOf(RandomUtil.generateNumber(0, 1));
            LocalDateTime cancelTime = null;
            
            if ("1".equals(isCancel)) {
                cancelTime = now.minusDays(RandomUtil.generateNumber(1, 30));
            }
            
            params.add(new Object[]{
                id, userId, skuId, spuId, isCancel,
                now, cancelTime
            });
        }
        
        dbUtil.batchInsert(sql, params);
    }
} 