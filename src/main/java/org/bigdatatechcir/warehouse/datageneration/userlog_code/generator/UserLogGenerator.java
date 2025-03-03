package org.bigdatatechcir.warehouse.datageneration.userlog_code.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.bigdatatechcir.warehouse.datageneration.userlog_code.model.*;
import java.util.*;
import java.time.Instant;

public class UserLogGenerator {
    private static final Random random = new Random();
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    private static final String[] AREAS = {"北京", "上海", "广州", "深圳", "杭州", "成都", "武汉"};
    private static final String[] BRANDS = {"华为", "小米", "OPPO", "vivo", "Apple", "三星", "魅族"};
    private static final String[] CHANNELS = {"小米商城", "华为商城", "oppo商城", "vivo商城", "苹果商城", "京东", "天猫"};
    private static final String[] OS = {"Android 11", "Android 12", "Android 13", "iOS 15", "iOS 16"};
    private static final String[] VERSIONS = {"1.0.0", "1.1.0", "1.2.0", "2.0.0", "2.1.0"};
    private static final String[] PAGE_IDS = {"home", "category", "cart", "detail", "pay", "order", "search", "user"};
    private static final String[] SOURCE_TYPES = {"promotion", "recommend", "search", "self", "advertisement"};
    private static final String[] ITEM_TYPES = {"sku", "spu", "category", "brand", "shop"};
    
    public static UserLog generateLog() {
        UserLog log = new UserLog();
        log.setCommon(generateCommon());
        log.setStart(generateStart());
        log.setPage(generatePage());
        log.setActions(generateActions());
        log.setDisplays(generateDisplays());
        log.setErr(generateError());
        log.setTs(Instant.now().toEpochMilli());
        return log;
    }
    
    private static Common generateCommon() {
        Common common = new Common();
        common.setAr(AREAS[random.nextInt(AREAS.length)]);
        common.setBa(BRANDS[random.nextInt(BRANDS.length)]);
        common.setCh(CHANNELS[random.nextInt(CHANNELS.length)]);
        common.setIs_new(String.valueOf(random.nextInt(2)));
        common.setMd(BRANDS[random.nextInt(BRANDS.length)] + "-" + random.nextInt(100));
        common.setMid(UUID.randomUUID().toString().replace("-", "").substring(0, 16));
        common.setOs(OS[random.nextInt(OS.length)]);
        common.setUid(String.format("%06d", random.nextInt(1000000)));
        common.setVc(VERSIONS[random.nextInt(VERSIONS.length)]);
        return common;
    }
    
    private static Start generateStart() {
        Start start = new Start();
        start.setEntry("entry_" + random.nextInt(5));
        start.setLoading_time(String.valueOf(random.nextInt(2000) + 500));
        start.setOpen_ad_id(String.format("ad_%03d", random.nextInt(100)));
        start.setOpen_ad_ms(String.valueOf(random.nextInt(5000) + 1000));
        start.setOpen_ad_skip_ms(String.valueOf(random.nextInt(3000)));
        return start;
    }
    
    private static Page generatePage() {
        Page page = new Page();
        page.setDuring_time((long) (random.nextInt(300000) + 1000));
        page.setItem(String.format("item_%06d", random.nextInt(1000000)));
        page.setItem_type(ITEM_TYPES[random.nextInt(ITEM_TYPES.length)]);
        page.setLast_page_id(PAGE_IDS[random.nextInt(PAGE_IDS.length)]);
        page.setPage_id(PAGE_IDS[random.nextInt(PAGE_IDS.length)]);
        page.setSource_type(SOURCE_TYPES[random.nextInt(SOURCE_TYPES.length)]);
        return page;
    }
    
    private static String generateActions() {
        List<Map<String, Object>> actions = new ArrayList<>();
        int actionCount = random.nextInt(5);
        for (int i = 0; i < actionCount; i++) {
            Map<String, Object> action = new HashMap<>();
            action.put("action_id", String.format("action_%03d", random.nextInt(100)));
            action.put("item_id", String.format("item_%06d", random.nextInt(1000000)));
            action.put("item_type", ITEM_TYPES[random.nextInt(ITEM_TYPES.length)]);
            action.put("ts", Instant.now().toEpochMilli());
            actions.add(action);
        }
        try {
            return objectMapper.writeValueAsString(actions);
        } catch (Exception e) {
            return "[]";
        }
    }
    
    private static String generateDisplays() {
        List<Map<String, Object>> displays = new ArrayList<>();
        int displayCount = random.nextInt(10);
        for (int i = 0; i < displayCount; i++) {
            Map<String, Object> display = new HashMap<>();
            display.put("display_type", String.format("type_%02d", random.nextInt(10)));
            display.put("item_id", String.format("item_%06d", random.nextInt(1000000)));
            display.put("item_type", ITEM_TYPES[random.nextInt(ITEM_TYPES.length)]);
            display.put("order", i + 1);
            display.put("pos_id", String.format("pos_%02d", random.nextInt(50)));
            displays.add(display);
        }
        try {
            return objectMapper.writeValueAsString(displays);
        } catch (Exception e) {
            return "[]";
        }
    }
    
    private static org.bigdatatechcir.warehouse.datageneration.userlog_code.model.Error generateError() {
        if (random.nextInt(100) < 95) {
            return null;
        }
        org.bigdatatechcir.warehouse.datageneration.userlog_code.model.Error error = new org.bigdatatechcir.warehouse.datageneration.userlog_code.model.Error();
        error.setError_code((long) random.nextInt(1000));
        error.setMsg("Error: " + UUID.randomUUID().toString().substring(0, 8));
        return error;
    }
} 