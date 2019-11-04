package com.cl.data.hbase.service;

import com.cl.data.hbase.dto.BlogAnalysisResultDTO;
import com.cl.data.hbase.dto.UidMidBlogDTO;
import com.cl.data.hbase.util.BlogCleanUtils;
import com.cl.graph.weibo.core.util.CsvFileHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

/**
 * @author yejianyu
 * @date 2019/9/4
 */
@Slf4j
@Service
public class UidMidBlogFileService {

    private static final String[] HEADER_UID_MID_BLOG = {"mid", "uid", "reposts_count", "comments_count",
        "attitudes_count", "text", "retweeted_text", "created_at"};

    private static final String[] HEADER_UID_MID_BLOG_FANS = {"mid", "uid", "reposts_count", "comments_count",
        "attitudes_count", "text", "retweeted_mid", "created_at"};

    @Resource
    private UidMidBlogHbaseService uidMidBlogHbaseService;

    @Resource(name = "hbaseThreadPoolTaskExecutor")
    private ThreadPoolTaskExecutor hbaseThreadPoolTaskExecutor;

    public void processFileResult(String filePath) {
        processFile(filePath, this::processOneFile);
    }

    public void processFansFileResult(String filePath) {
        processFile(filePath, this::processOneFansFile);
    }

    private void processFile(String filePath, Consumer<File> consumer) {
        File file = new File(filePath);
        if (file.isDirectory()){
            File[] files = file.listFiles();
            if (files == null || files.length == 0){
                return;
            }
            CountDownLatch countDownLatch = new CountDownLatch(files.length);
            for (File oneFile : files){
                hbaseThreadPoolTaskExecutor.execute(() -> {
                    try {
                        consumer.accept(oneFile);
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            consumer.accept(file);
        }
    }

    private void processOneFile(File file) {
        log.info("开始处理博文文件{}", file.getPath());
        long startTime = System.currentTimeMillis();
        int count = 0;
        List<UidMidBlogDTO> list = new ArrayList<>();
        try (CSVParser csvParser = CsvFileHelper.reader(file, HEADER_UID_MID_BLOG, true)) {
//        try (CSVParser csvParser = CsvFileHelper.reader(file, HEADER_UID_MID_BLOG)) {
            for (CSVRecord record : csvParser) {
                String uid = record.get("uid");
                String mid = record.get("mid");
                String createdAt = record.get("created_at");
                UidMidBlogDTO uidMidBlogDTO = new UidMidBlogDTO();
                uidMidBlogDTO.setUid(uid);
                uidMidBlogDTO.setMid(mid);
                uidMidBlogDTO.setCreateTime(createdAt);
                uidMidBlogDTO.setRepostsCount(Long.valueOf(record.get("reposts_count")));
                uidMidBlogDTO.setCommentsCount(Long.valueOf(record.get("comments_count")));
                uidMidBlogDTO.setAttitudesCount(Long.valueOf(record.get("attitudes_count")));
                String text = BlogCleanUtils.cleanBlog(record.get("text"));
                String retweetedText = BlogCleanUtils.cleanBlog(record.get("retweeted_text"));
                uidMidBlogDTO.setText(text);
                uidMidBlogDTO.setRetweetedText(retweetedText);
                list.add(uidMidBlogDTO);
                count++;
                if (count % 5000 == 0) {
                    uidMidBlogHbaseService.batchInsertUidMidBlog(list);
                    list.clear();
                }
                if (count % 100000 == 0) {
                    log.info("处理博文文件, filePath={}, progress={}, time={}ms", file.getPath(), count,
                            System.currentTimeMillis() - startTime);
                }
            }
            if (list.size() > 0) {
                uidMidBlogHbaseService.batchInsertUidMidBlog(list);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("处理博文文件完成, filePath={}, total={}, time={}ms", file.getPath(), count,
                System.currentTimeMillis() - startTime);
    }

    private void processOneFansFile(File file) {
        log.info("开始处理博文文件{}", file.getPath());
        long startTime = System.currentTimeMillis();
        int count = 0;
        List<UidMidBlogDTO> list = new ArrayList<>();
        try (CSVParser csvParser = CsvFileHelper.reader(file, HEADER_UID_MID_BLOG_FANS, true)) {
//        try (CSVParser csvParser = CsvFileHelper.reader(file, HEADER_UID_MID_BLOG)) {
            for (CSVRecord record : csvParser) {
                String uid = record.get("uid");
                String mid = record.get("mid");
                String createdAt = record.get("created_at");
                UidMidBlogDTO uidMidBlogDTO = new UidMidBlogDTO();
                uidMidBlogDTO.setUid(uid);
                uidMidBlogDTO.setMid(mid);
                uidMidBlogDTO.setCreateTime(createdAt);
                uidMidBlogDTO.setRepostsCount(Long.valueOf(record.get("reposts_count")));
                uidMidBlogDTO.setCommentsCount(Long.valueOf(record.get("comments_count")));
                uidMidBlogDTO.setAttitudesCount(Long.valueOf(record.get("attitudes_count")));
                String text = BlogCleanUtils.cleanBlog(record.get("text"));
                uidMidBlogDTO.setText(text);
                String retweetedMid = record.get("retweeted_mid");
                String isRetweeted = "0";
                if (StringUtils.isNotBlank(retweetedMid)){
                    isRetweeted = "1";
                }
                uidMidBlogDTO.setIsRetweeted(isRetweeted);
                list.add(uidMidBlogDTO);
                count++;
                if (count % 5000 == 0) {
                    uidMidBlogHbaseService.batchInsertUidMidBlog(list);
                    list.clear();
                }
                if (count % 100000 == 0) {
                    log.info("处理博文文件, filePath={}, progress={}, time={}ms", file.getPath(), count,
                        System.currentTimeMillis() - startTime);
                }
            }
            if (list.size() > 0) {
                uidMidBlogHbaseService.batchInsertUidMidBlog(list);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("处理博文文件完成, filePath={}, total={}, time={}ms", file.getPath(), count,
            System.currentTimeMillis() - startTime);
    }
}
