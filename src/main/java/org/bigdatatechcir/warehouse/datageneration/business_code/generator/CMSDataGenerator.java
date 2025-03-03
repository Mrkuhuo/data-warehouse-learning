package org.bigdatatechcir.warehouse.datageneration.business_code.generator;

import org.bigdatatechcir.warehouse.datageneration.business_code.util.DbUtil;
import org.bigdatatechcir.warehouse.datageneration.business_code.util.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class CMSDataGenerator {
    private static final Logger logger = LoggerFactory.getLogger(CMSDataGenerator.class);

    public static void generateCMSData(int batchSize) {
        generateCmsBanner(batchSize);
    }

    private static void generateCmsBanner(int batchSize) {
        String sql = "INSERT INTO cms_banner (id, title, img_url, url, sort, is_show) VALUES (?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= batchSize; i++) {
            String title = "广告banner" + i;
            String imgUrl = "http://example.com/banners/" + i + ".jpg";
            String url = "http://example.com/promotion/" + i;
            int sort = i;
            String isShow = "1";

            params.add(new Object[]{i, title, imgUrl, url, sort, isShow});
        }
        DbUtil.batchInsert(sql, params);
    }
} 