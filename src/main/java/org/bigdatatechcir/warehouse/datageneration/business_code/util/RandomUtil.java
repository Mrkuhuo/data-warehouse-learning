package org.bigdatatechcir.warehouse.datageneration.business_code.util;

import org.springframework.stereotype.Component;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import java.util.UUID;

@Component
public class RandomUtil {
    private static final Random random = new Random();
    private static final String[] FIRST_NAMES = {"张", "王", "李", "赵", "刘", "陈", "杨", "黄", "周", "吴"};
    private static final String[] LAST_NAMES = {"伟", "芳", "娜", "秀英", "敏", "静", "丽", "强", "磊", "洋"};
    private static final String[] NICK_PREFIXES = {"快乐", "阳光", "微笑", "温暖", "幸福", "美好", "甜蜜", "开心", "快乐", "幸福"};
    private static final String[] NICK_SUFFIXES = {"小天使", "小太阳", "小星星", "小月亮", "小彩虹", "小花朵", "小蝴蝶", "小蜜蜂", "小海豚", "小猫咪"};
    private static final String[] PRODUCT_PREFIXES = {"时尚", "经典", "潮流", "优雅", "简约", "现代", "复古", "奢华", "清新", "自然"};
    private static final String[] PRODUCT_SUFFIXES = {"连衣裙", "T恤", "衬衫", "外套", "裤子", "鞋子", "包包", "帽子", "围巾", "手套"};
    private static final String[] COLORS = {"红色", "蓝色", "绿色", "黄色", "紫色", "粉色", "白色", "黑色", "灰色", "棕色"};
    private static final String[] BRANDS = {"Nike", "Adidas", "Puma", "New Balance", "Under Armour", "Reebok", "Converse", "Vans", "Fila", "Asics"};
    public static final String[] PROVINCES = {"北京", "上海", "广东", "江苏", "浙江", "山东", "河南", "四川", "湖北", "湖南"};
    public static final String[] CITIES = {"北京", "上海", "广州", "深圳", "杭州", "南京", "成都", "武汉", "长沙", "郑州"};
    
    public static String generateName() {
        return FIRST_NAMES[random.nextInt(FIRST_NAMES.length)] + LAST_NAMES[random.nextInt(LAST_NAMES.length)];
    }
    
    public static String generateNickName() {
        return NICK_PREFIXES[random.nextInt(NICK_PREFIXES.length)] + NICK_SUFFIXES[random.nextInt(NICK_SUFFIXES.length)];
    }
    
    public static String generatePassword() {
        return "password" + random.nextInt(1000);
    }
    
    public static String generatePhone() {
        return "1" + random.nextInt(9) + random.nextInt(10) + random.nextInt(10) + 
               random.nextInt(10) + random.nextInt(10) + random.nextInt(10) + 
               random.nextInt(10) + random.nextInt(10) + random.nextInt(10) + 
               random.nextInt(10) + random.nextInt(10);
    }
    
    public static String generateEmail() {
        return "user" + random.nextInt(1000) + "@example.com";
    }
    
    public static String generateBirthday() {
        LocalDate start = LocalDate.of(1970, 1, 1);
        LocalDate end = LocalDate.of(2000, 12, 31);
        long days = ChronoUnit.DAYS.between(start, end);
        LocalDate randomDate = start.plusDays(random.nextInt((int) days));
        return randomDate.toString();
    }
    
    public static String generateGender() {
        return random.nextInt(2) == 0 ? "1" : "2"; // 1:男 2:女
    }
    
    public static String generateAddress() {
        return PROVINCES[random.nextInt(PROVINCES.length)] + "市" + 
               random.nextInt(100) + "区" + random.nextInt(100) + "号";
    }
    
    public static String generateProductName() {
        return PRODUCT_PREFIXES[random.nextInt(PRODUCT_PREFIXES.length)] + 
               PRODUCT_SUFFIXES[random.nextInt(PRODUCT_SUFFIXES.length)];
    }
    
    public static BigDecimal generatePrice(double min, double max) {
        double randomValue = min + (max - min) * random.nextDouble();
        return new BigDecimal(randomValue).setScale(2, RoundingMode.HALF_UP);
    }
    
    public static int generateNumber(int min, int max) {
        return random.nextInt(max - min + 1) + min;
    }
    
    public static String generateColor() {
        return COLORS[random.nextInt(COLORS.length)];
    }
    
    public static String generateBrand() {
        return BRANDS[random.nextInt(BRANDS.length)];
    }
    
    public static String generateOrderNo() {
        return String.format("%d%d", System.currentTimeMillis(), generateNumber(1000, 9999));
    }
    
    public static String generateSku() {
        return String.format("SKU%06d", generateNumber(1, 999999));
    }
    
    public static String generateUUID() {
        return UUID.randomUUID().toString().replace("-", "");
    }
    
    public static LocalDateTime generateDateTime() {
        LocalDateTime now = LocalDateTime.now();
        return now.minusDays(random.nextInt(365))
                  .minusHours(random.nextInt(24))
                  .minusMinutes(random.nextInt(60))
                  .minusSeconds(random.nextInt(60));
    }
    
    public static String generateSpuCode() {
        return "SPU" + String.format("%06d", random.nextInt(1000000));
    }
    
    public static String generateCategory() {
        String[] categories = {"服装", "鞋靴", "箱包", "电子产品", "家居用品", "食品", "美妆", "运动户外"};
        return categories[random.nextInt(categories.length)];
    }

    public static double generateDouble(int min, int max) {
        return min + (max - min) * random.nextDouble();
    }
} 