package org.bigdatatechcir.warehouse.datageneration.business_code.generator;

import org.bigdatatechcir.warehouse.datageneration.business_code.util.DbUtil;
import org.bigdatatechcir.warehouse.datageneration.business_code.util.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

@Component
public class UserDataGenerator {
    private static final Logger logger = LoggerFactory.getLogger(UserDataGenerator.class);
    private static final int BATCH_SIZE = 500;

    @Autowired
    private DbUtil dbUtil;

    public void generateUserData(int count) {
        generateUserInfo(count);
        generateUserAddress(count);
    }

    private void generateUserInfo(int count) {
        // 获取最大ID
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM user_info";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO user_info (id, login_name, nick_name, passwd, name, phone_num, email, " +
                    "head_img, user_level, birthday, gender, create_time, operate_time) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            String loginName = "user" + id;
            String nickName = "用户" + id;
            String passwd = "password" + id;
            String name = RandomUtil.generateName();
            String phoneNum = RandomUtil.generatePhone();
            String email = "user" + id + "@example.com";
            String headImg = "http://example.com/avatars/user" + id + ".jpg";
            int userLevel = RandomUtil.generateNumber(1, 3);
            LocalDate birthday = LocalDate.now().minusYears(RandomUtil.generateNumber(18, 60));
            String gender = String.valueOf(RandomUtil.generateNumber(1, 2));
            LocalDateTime now = LocalDateTime.now();
            
            params.add(new Object[]{
                id, loginName, nickName, passwd, name, phoneNum, email,
                headImg, userLevel, birthday, gender, now, now
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateUserAddress(int count) {
        String sql = "INSERT INTO user_address (id, user_id, province_id, user_address, consignee, phone_num, " +
                    "is_default) VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        int remainingCount = count;
        while (remainingCount > 0) {
            // 获取最大ID
            String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM user_address";
            int startId = dbUtil.queryForInt(maxIdSql) + 1;
            
            int batchCount = Math.min(remainingCount, BATCH_SIZE);
            List<Object[]> params = new ArrayList<>();
            
            for (int i = 0; i < batchCount; i++) {
                int id = startId + i;
                int userId = RandomUtil.generateNumber(1, 1000);
                int provinceId = RandomUtil.generateNumber(1, 34);
                String userAddress = RandomUtil.generateAddress();
                String consignee = RandomUtil.generateName();
                String phoneNum = RandomUtil.generatePhone();
                String isDefault = i == 0 ? "1" : "0";
                
                params.add(new Object[]{
                    id, userId, provinceId, userAddress, consignee, phoneNum, isDefault
                });
            }
            
            dbUtil.batchInsert(sql, params);
            remainingCount -= batchCount;
        }
    }
} 