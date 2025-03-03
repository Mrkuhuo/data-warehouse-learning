package org.bigdatatechcir.warehouse.datageneration.business_code.generator;

import org.bigdatatechcir.warehouse.datageneration.business_code.util.DbUtil;
import org.bigdatatechcir.warehouse.datageneration.business_code.util.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class UserBehaviorGenerator {
    private static final Logger logger = LoggerFactory.getLogger(UserBehaviorGenerator.class);

    public static void generateUserBehavior(int batchSize) {
        generateCartInfo(batchSize);
        generateCommentInfo(batchSize);
        generateFavorInfo(batchSize);
        generateUserInfo(batchSize);
        generateUserAddress(batchSize);
    }

    private static void generateCartInfo(int batchSize) {
        String sql = "INSERT INTO cart_info (id, user_id, sku_id, cart_price, sku_num, img_url, sku_name, " +
                    "is_checked, create_time, operate_time, is_ordered, order_time, source_type, source_id) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            int userId = RandomUtil.generateNumber(1, 1000);
            String skuId = RandomUtil.generateSku();
            BigDecimal cartPrice = RandomUtil.generatePrice(10, 1000);
            int skuNum = RandomUtil.generateNumber(1, 5);
            String imgUrl = "http://example.com/images/" + skuId + ".jpg";
            String skuName = "商品" + i;
            String isChecked = "1";
            LocalDateTime createTime = LocalDateTime.now();
            LocalDateTime operateTime = createTime;
            String isOrdered = "0";
            LocalDateTime orderTime = null;
            String sourceType = "1";
            String sourceId = "0";

            params.add(new Object[]{
                i, userId, skuId, cartPrice, skuNum, imgUrl, skuName, isChecked,
                createTime, operateTime, isOrdered, orderTime, sourceType, sourceId
            });
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateCommentInfo(int batchSize) {
        String sql = "INSERT INTO comment_info (id, user_id, nick_name, head_img, sku_id, spu_id, order_id, " +
                    "appraise, comment_txt, create_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            int userId = RandomUtil.generateNumber(1, 1000);
            String nickName = RandomUtil.generateName();
            String headImg = "http://example.com/avatar/" + userId + ".jpg";
            String skuId = RandomUtil.generateSku();
            String spuId = RandomUtil.generateSpuCode();
            String orderId = RandomUtil.generateOrderNo();
            String appraise = String.valueOf(RandomUtil.generateNumber(1, 5));
            String commentTxt = "很好的商品，推荐购买！" + i;
            LocalDateTime createTime = LocalDateTime.now();

            params.add(new Object[]{
                i, userId, nickName, headImg, skuId, spuId, orderId, appraise,
                commentTxt, createTime
            });
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateFavorInfo(int batchSize) {
        String sql = "INSERT INTO favor_info (id, user_id, sku_id, spu_id, is_cancel, create_time, cancel_time) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            int userId = RandomUtil.generateNumber(1, 1000);
            String skuId = RandomUtil.generateSku();
            String spuId = RandomUtil.generateSpuCode();
            String isCancel = "0";
            LocalDateTime createTime = LocalDateTime.now();
            LocalDateTime cancelTime = null;

            params.add(new Object[]{
                i, userId, skuId, spuId, isCancel, createTime, cancelTime
            });
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateUserInfo(int batchSize) {
        String sql = "INSERT INTO user_info (id, login_name, nick_name, passwd, name, phone_num, email, " +
                    "head_img, user_level, birthday, gender, create_time, operate_time) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            String name = RandomUtil.generateName();
            String loginName = "user" + i;
            String nickName = name;
            String passwd = RandomUtil.generateUUID().substring(0, 16);
            String phoneNum = RandomUtil.generatePhone();
            String email = RandomUtil.generateEmail(name);
            String headImg = "http://example.com/avatar/" + i + ".jpg";
            String userLevel = String.valueOf(RandomUtil.generateNumber(1, 5));
            LocalDateTime birthday = RandomUtil.generateDateTime(
                LocalDateTime.of(1970, 1, 1, 0, 0),
                LocalDateTime.of(2000, 12, 31, 23, 59)
            );
            String gender = String.valueOf(RandomUtil.generateNumber(0, 1));
            LocalDateTime createTime = LocalDateTime.now();
            LocalDateTime operateTime = createTime;

            params.add(new Object[]{
                i, loginName, nickName, passwd, name, phoneNum, email, headImg,
                userLevel, birthday, gender, createTime, operateTime
            });
        }
        DbUtil.batchInsert(sql, params);
    }

    private static void generateUserAddress(int batchSize) {
        String sql = "INSERT INTO user_address (id, user_id, province_id, user_address, consignee, phone_num, " +
                    "is_default) VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            int userId = RandomUtil.generateNumber(1, 1000);
            int provinceId = RandomUtil.generateNumber(1, 34);
            String address = RandomUtil.generateAddress();
            String consignee = RandomUtil.generateName();
            String phoneNum = RandomUtil.generatePhone();
            String isDefault = i == 1 ? "1" : "0";

            params.add(new Object[]{
                i, userId, provinceId, address, consignee, phoneNum, isDefault
            });
        }
        DbUtil.batchInsert(sql, params);
    }
} 