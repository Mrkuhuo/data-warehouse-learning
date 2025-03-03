package org.bigdatatechcir.warehouse.datageneration.business_code.util;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.UUID;

public class RandomUtil {
    private static final Random random = new Random();
    private static final String[] FIRST_NAMES = {"张", "王", "李", "赵", "刘", "陈", "杨", "黄", "周", "吴"};
    private static final String[] LAST_NAMES = {"伟", "芳", "娜", "秀英", "敏", "静", "丽", "强", "磊", "洋"};
    public static final String[] PROVINCES = {"北京", "上海", "广东", "浙江", "江苏", "四川", "湖北", "湖南", "河南", "河北"};
    public static final String[] CITIES = {"北京", "上海", "广州", "深圳", "杭州", "南京", "成都", "武汉", "长沙", "郑州"};
    
    public static String generateName() {
        return FIRST_NAMES[random.nextInt(FIRST_NAMES.length)] + 
               LAST_NAMES[random.nextInt(LAST_NAMES.length)];
    }
    
    public static String generatePhone() {
        return "1" + (random.nextInt(7) + 3) + String.format("%09d", random.nextInt(1000000000));
    }
    
    public static String generateEmail(String name) {
        String[] domains = {"gmail.com", "yahoo.com", "hotmail.com", "163.com", "qq.com"};
        return name + random.nextInt(1000) + "@" + domains[random.nextInt(domains.length)];
    }
    
    public static String generateAddress() {
        return PROVINCES[random.nextInt(PROVINCES.length)] + "省" +
               CITIES[random.nextInt(CITIES.length)] + "市" +
               "某某区某某街道" + random.nextInt(100) + "号";
    }
    
    public static BigDecimal generatePrice(double min, double max) {
        double price = min + (max - min) * random.nextDouble();
        return new BigDecimal(String.format("%.2f", price));
    }
    
    public static String generateOrderNo() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")) +
               String.format("%04d", random.nextInt(10000));
    }
    
    public static String generateSku() {
        return "SKU" + String.format("%06d", random.nextInt(1000000));
    }
    
    public static String generateSpuCode() {
        return "SPU" + String.format("%06d", random.nextInt(1000000));
    }
    
    public static String generateUUID() {
        return UUID.randomUUID().toString().replace("-", "");
    }
    
    public static LocalDateTime generateDateTime(LocalDateTime start, LocalDateTime end) {
        long startEpochSecond = start.toEpochSecond(java.time.ZoneOffset.UTC);
        long endEpochSecond = end.toEpochSecond(java.time.ZoneOffset.UTC);
        long randomEpochSecond = startEpochSecond + 
            (long) (random.nextDouble() * (endEpochSecond - startEpochSecond));
        return LocalDateTime.ofEpochSecond(randomEpochSecond, 0, java.time.ZoneOffset.UTC);
    }
    
    public static int generateNumber(int min, int max) {
        return min + random.nextInt(max - min + 1);
    }
    
    public static String generateColor() {
        String[] colors = {"红色", "蓝色", "黑色", "白色", "灰色", "金色", "银色", "粉色"};
        return colors[random.nextInt(colors.length)];
    }
    
    public static String generateSize() {
        String[] sizes = {"S", "M", "L", "XL", "XXL", "均码"};
        return sizes[random.nextInt(sizes.length)];
    }
    
    public static String generateBrand() {
        String[] brands = {"Nike", "Adidas", "Puma", "李宁", "安踏", "特步", "耐克", "阿迪达斯"};
        return brands[random.nextInt(brands.length)];
    }
    
    public static String generateCategory() {
        String[] categories = {"服装", "鞋靴", "箱包", "电子产品", "家居用品", "食品", "美妆", "运动户外"};
        return categories[random.nextInt(categories.length)];
    }
} 