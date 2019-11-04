package com.cl.data.hbase.service;

import com.cl.data.hbase.entity.MblogFromUid;
import com.cl.data.hbase.mapper.marketing.MblogFromUidMapper;
import com.cl.graph.weibo.core.exception.ServiceException;
import com.cl.graph.weibo.core.util.CsvFileHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author yejianyu
 * @date 2019/7/16
 */
@Slf4j
@Service
public class MblogFromUidService {

    @Resource
    private UidMidBlogHbaseService uidMidBlogHbaseService;

    @Resource
    private MblogFromUidMapper mblogFromUidMapper;

    @Resource(name = "hbaseThreadPoolTaskExecutor")
    private ThreadPoolTaskExecutor hbaseThreadPoolTaskExecutor;

    public void processBidToHbase(String tableName) throws ServiceException {

        checkTable(tableName);

        long startTime = System.currentTimeMillis();
        log.info("开始保存博文Bid到hbase，table={}", tableName);
        long maxCount = mblogFromUidMapper.count(tableName);
        log.info("count {}, size={}", tableName, maxCount);
        AtomicInteger count = new AtomicInteger();
        int pageSize = 5000;
        int pageNum = 1;
        int pageNumMax = (int) Math.ceil((double) maxCount / pageSize);
        CountDownLatch countDownLatch = new CountDownLatch(pageNumMax);
        while (pageNum <= pageNumMax) {
            List<MblogFromUid> blogList = mblogFromUidMapper.findAllBid(tableName, pageNum, pageSize);
            hbaseThreadPoolTaskExecutor.execute(() -> {
                try {
                    uidMidBlogHbaseService.batchInsertUidMidBid(blogList);
                    int c = count.addAndGet(blogList.size());
                    if (c % 50000 == 0) {
                        log.info("插入hbase，{}已处理:{}/{}, 已耗时{}ms", tableName, c, maxCount,
                            System.currentTimeMillis() - startTime);
                    }
                } finally {
                    countDownLatch.countDown();
                }
            });
            pageNum++;
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        log.info("保存博文Bid到hbase完成, table = {}，total={}, time={}ms", tableName, count,
            System.currentTimeMillis() - startTime);
    }


    public void processIsRetweetedToHbase(String tableName) throws ServiceException {

        checkTable(tableName);

        long startTime = System.currentTimeMillis();
        log.info("开始保存博文is_retweeted到hbase，table={}", tableName);
        long maxCount = mblogFromUidMapper.count(tableName);
        log.info("count {}, size={}", tableName, maxCount);
        AtomicInteger count = new AtomicInteger();
        int pageSize = 5000;
        int pageNum = 1;
        int pageNumMax = (int) Math.ceil((double) maxCount / pageSize);
        CountDownLatch countDownLatch = new CountDownLatch(pageNumMax);
        while (pageNum <= pageNumMax) {
            List<MblogFromUid> blogList = mblogFromUidMapper.findAllRetweetedMid(tableName, pageNum, pageSize);
            hbaseThreadPoolTaskExecutor.execute(() -> {
                try {
                    uidMidBlogHbaseService.batchInsertUidMidIsRetweeted(blogList);
                    int c = count.addAndGet(blogList.size());
                    if (c % 50000 == 0) {
                        log.info("插入hbase，{}已处理:{}/{}, 已耗时{}ms", tableName, c, maxCount,
                            System.currentTimeMillis() - startTime);
                    }
                } finally {
                    countDownLatch.countDown();
                }
            });
            pageNum++;
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        log.info("保存博文is_retweeted到hbase完成, table = {}，total={}, time={}ms", tableName, count,
            System.currentTimeMillis() - startTime);
    }

    public void processIsRetweetedToHbaseFromFile(String filePath) throws ServiceException {

        long startTime = System.currentTimeMillis();
        log.info("开始保存博文is_retweeted到hbase，filePath={}", filePath);
        int count = 0;
        int allCount = 0;
        String[] header = {"mid", "uid", "retweeted_mid"};
//        String[] header = {"mid","uid","reposts_count","comments_count","attitudes_count","text","retweeted_mid","created_at"};
        List<MblogFromUid> list = new ArrayList<>();
        try (CSVParser csvParser = CsvFileHelper.reader(filePath,header,true)) {
            for (CSVRecord record : csvParser) {
                MblogFromUid mblogFromUid = new MblogFromUid();
                mblogFromUid.setUid(record.get(1));
                mblogFromUid.setMid(record.get(0));
                mblogFromUid.setRetweetedMid(record.get(2));
                list.add(mblogFromUid);
                count++;
                allCount++;
                if (count % 50000 == 0) {
                    uidMidBlogHbaseService.batchInsertUidMidIsRetweeted(list);
                    log.info("插入hbase，{}已处理:{}, 已耗时{}ms", filePath, allCount,
                        System.currentTimeMillis() - startTime);
                    count = 0;
                    list.clear();
                }
            }
            if (!list.isEmpty()){
                uidMidBlogHbaseService.batchInsertUidMidIsRetweeted(list);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        log.info("保存博文is_retweeted到hbase完成, filePath = {}，total={}, time={}ms", filePath, allCount,
            System.currentTimeMillis() - startTime);
    }

    private void checkTable(String tableName) throws ServiceException {
        int tableExist = mblogFromUidMapper.isTableExist(tableName);
        if (tableExist <= 0) {
            String errorMsg = "表 " + tableName + " 不存在";
            log.error(errorMsg);
            throw new ServiceException(errorMsg);
        }
    }
}
