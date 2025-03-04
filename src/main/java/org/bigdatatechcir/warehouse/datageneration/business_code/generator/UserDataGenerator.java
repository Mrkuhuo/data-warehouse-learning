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

    @Autowired
    private DbUtil dbUtil;

    public void generateUserData(int count) {
        generateUserInfo(count);
        generateUserAddress(count);
    }

    private void generateUserInfo(int count) {
        logger.info("正在更新 user_info 表...");
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM user_info";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO user_info (id, login_name, nick_name, passwd, name, phone_num, " +
                    "email, head_img, user_level, birthday, gender, create_time, operate_time) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            LocalDateTime now = LocalDateTime.now();
            
            String loginName = "user" + id;
            String nickName = "用户" + id;
            String name = RandomUtil.generateName();
            String phoneNum = "1" + RandomUtil.generateNumber(3, 9) + 
                            String.format("%08d", RandomUtil.generateNumber(0, 99999999));
            String email = loginName + "@example.com";
            String headImg = "http://example.com/avatar/" + id + ".jpg";
            int userLevel = RandomUtil.generateNumber(1, 5);  // 1-5级会员
            LocalDate birthday = LocalDate.now().minusYears(RandomUtil.generateNumber(18, 60));
            String gender = String.valueOf(RandomUtil.generateNumber(1, 2));  // 1-男，2-女
            
            params.add(new Object[]{
                id,
                loginName,
                nickName,
                "e10adc3949ba59abbe56e057f20f883e",  // 123456的MD5
                name,
                phoneNum,
                email,
                headImg,
                userLevel,
                birthday,
                gender,
                now,
                now
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateUserAddress(int count) {
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM user_address";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO user_address (id, user_id, province_id, user_address, consignee, " +
                    "phone_num, is_default) VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            int userId = RandomUtil.generateNumber(1, count);  // 关联已存在的用户
            int provinceId = RandomUtil.generateNumber(1, 34);  // 关联已存在的省份
            String address = "XX市XX区XX街道" + RandomUtil.generateNumber(1, 999) + "号";
            String consignee = RandomUtil.generateName();
            String phoneNum = "1" + RandomUtil.generateNumber(3, 9) + 
                            String.format("%08d", RandomUtil.generateNumber(0, 99999999));
            String isDefault = i % 5 == 0 ? "1" : "0";  // 每5个地址中有1个是默认地址
            
            params.add(new Object[]{
                id,
                userId,
                provinceId,
                address,
                consignee,
                phoneNum,
                isDefault
            });
        }
        dbUtil.batchInsert(sql, params);
    }
} 