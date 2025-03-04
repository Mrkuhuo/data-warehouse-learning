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
public class CMSDataGenerator {
    private static final Logger logger = LoggerFactory.getLogger(CMSDataGenerator.class);

    @Autowired
    private DbUtil dbUtil;

    public void generateCMSData(int bannerCount, int subjectCount, int commentCount) {
        generateBanner(bannerCount);
        generateSpuPoster(subjectCount);
    }

    private void generateBanner(int count) {
        logger.info("正在更新 cms_banner 表...");
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM cms_banner";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO cms_banner (id, title, image_url, link_url, sort) " +
                    "VALUES (?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            String title = "Banner" + id;
            String imageUrl = "http://example.com/banners/banner" + id + ".jpg";
            String linkUrl = "http://example.com/products/" + RandomUtil.generateNumber(1, 1000);
            int sort = i + 1;  // 排序从1开始
            
            params.add(new Object[]{
                id,
                title,
                imageUrl,
                linkUrl,
                sort
            });
        }
        dbUtil.batchInsert(sql, params);
    }

    private void generateSpuPoster(int count) {
        logger.info("正在更新 spu_poster 表...");
        String maxIdSql = "SELECT COALESCE(MAX(id), 0) FROM spu_poster";
        int startId = dbUtil.queryForInt(maxIdSql) + 1;
        
        String sql = "INSERT INTO spu_poster (id, spu_id, img_name, img_url, create_time, " +
                    "update_time) VALUES (?, ?, ?, ?, ?, ?)";
        
        List<Object[]> params = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            int spuId = RandomUtil.generateNumber(1, 1000);  // 关联已存在的SPU
            String imgName = "商品海报" + id;
            String imgUrl = "http://example.com/posters/poster" + id + ".jpg";
            LocalDateTime now = LocalDateTime.now();
            LocalDateTime updateTime = now.minusDays(RandomUtil.generateNumber(1, 30));
            
            params.add(new Object[]{
                id,
                spuId,
                imgName,
                imgUrl,
                updateTime,
                now
            });
        }
        dbUtil.batchInsert(sql, params);
    }
} 