package com.cl.graph.weibo.data.service;

import com.cl.graph.weibo.core.constant.CommonConstants;
import com.cl.graph.weibo.core.constant.FileSuffixConstants;
import com.cl.graph.weibo.data.dto.RetweetDTO;
import com.cl.graph.weibo.data.entity.MblogFromUid;
import com.cl.graph.weibo.data.entity.MidUidText;
import com.cl.graph.weibo.data.entity.UserInfo;
import com.cl.graph.weibo.data.manager.CsvFile;
import com.cl.graph.weibo.data.manager.CsvFileManager;
import com.cl.graph.weibo.data.manager.RedisDataManager;
import com.cl.graph.weibo.data.mapper.marketing.MblogFromUidMapper;
import com.cl.graph.weibo.data.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

/**
 * @author yejianyu
 * @date 2019/7/16
 */
@Slf4j
@Service
public class MblogFromUidService {

    private final RedisDataManager redisDataManager;
    private final CsvFileManager csvFileManager;

    @Resource(name = "commonThreadPoolTaskExecutor")
    private ThreadPoolTaskExecutor taskExecutor;

    @Resource
    private MblogFromUidMapper mblogFromUidMapper;

    @Resource
    private MidUidTextService midUidTextService;

    @Value(value = "${isUserTypeFromFile}")
    private boolean isUserTypeFromFile;

    @Value(value = "${data.path.mblog}")
    private String mblogDataPath;

    private static final String RETWEET_FILE_DIC = "retweet";
    private static final String RELATION_RETWEET = "_retweet_";
    private static final Set<String> MID_SET = Collections.synchronizedSet(new HashSet<>());
    private static final String[] HEADER_RETWEET = {"from", "to", "mid", "createTime"};
    private static final String[] HEADER_BLOG = {"uid", "mid", "text", "createTime", "attitudesCount", "commentsCount",
            "repostsCount"};

    public MblogFromUidService(CsvFileManager csvFileManager, RedisDataManager redisDataManager) {
        this.csvFileManager = csvFileManager;
        this.redisDataManager = redisDataManager;
    }

    public void genAllBlogFileByDate(String startDate, String endDate) {

        List<String> dateList = DateTimeUtils.getDateList(startDate, endDate);
        int indexSize = 10;
        CountDownLatch countDownLatch = new CountDownLatch(dateList.size() * indexSize);
        for (String date : dateList) {
            for (int i = 0; i < indexSize; i++) {
                String tableSuffix = date + CommonConstants.UNDERLINE + i;
                taskExecutor.execute(() -> {
                    try {
                        genAllBlogFileBySuffix(tableSuffix);
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void genAllBlogFileByDateDetail(String startDate, String endDate) {
        List<String> dateList = DateTimeUtils.getDateList(startDate, endDate);
        CountDownLatch countDownLatch = new CountDownLatch(dateList.size());
        for (String date : dateList) {
            String tableSuffix = "detail" + CommonConstants.UNDERLINE + date;
            taskExecutor.execute(() -> {
                try {
                    genAllBlogFileBySuffix(tableSuffix);
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
    }

    public void genAllBlogFileBySuffix(String tableSuffix) {
        int pageSize = 100000;
        long maxCount;
        long startTime = System.currentTimeMillis();
        log.info("开始处理mblog_from_uid_{}", tableSuffix);
        try {
            maxCount = mblogFromUidMapper.count(tableSuffix);
        } catch (InvalidDataAccessResourceUsageException e) {
            processDatabaseException(e, tableSuffix);
            return;
        }
        log.info("统计mblog_from_uid_{}的总数为{}, 耗时{}ms", tableSuffix, maxCount,
                System.currentTimeMillis() - startTime);
        int count = 0;
        int pageNum = 1;
        File resultDir = new File(mblogDataPath);
        if (!resultDir.exists()) {
            resultDir.mkdirs();
        }
        String resultFile = resultDir.getPath() + File.separator + "mblog_from_uid_" + tableSuffix
                + FileSuffixConstants.CSV;
        try (CSVPrinter csvPrinter = CsvFileHelper.writer(resultFile, HEADER_BLOG)) {
            while (count < maxCount) {
                List<MblogFromUid> blogList = mblogFromUidMapper.findAll(tableSuffix, pageNum, pageSize);
                for (MblogFromUid mblogFromUid : blogList) {
                    csvPrinter.printRecord(mblogFromUid.getUid(), mblogFromUid.getMid(), mblogFromUid.getText(),
                            mblogFromUid.getCreateTime(), mblogFromUid.getAttitudesCount(),
                            mblogFromUid.getCommentsCount(), mblogFromUid.getRepostsCount());
                }
                count += blogList.size();
                pageNum++;
                log.info("mblog_from_uid_{}已处理:{}/{}, 已耗时{}ms", tableSuffix, count, maxCount,
                        System.currentTimeMillis() - startTime);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("mblog_from_uid_{}已处理完成，实际生成{}条数据, 共耗时{}ms", tableSuffix, count,
                System.currentTimeMillis() - startTime);
    }

    public void genBlogFileDepenadUidByDate(String startDate, String endDate) {

        List<String> dateList = DateTimeUtils.getDateList(startDate, endDate);
        int indexSize = 10;
        CountDownLatch countDownLatch = new CountDownLatch(dateList.size() * indexSize);
        for (String date : dateList) {
            for (int i = 0; i < indexSize; i++) {
                String tableSuffix = date + CommonConstants.UNDERLINE + i;
                taskExecutor.execute(() -> {
                    try {
                        genBlogFileDepenadUidBySuffix(tableSuffix);
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void insertBlogDepenadUidByDate(String startDate, String endDate) {
        processMblogFromUidDataByDate(startDate, endDate, this::insertBlogDepenadUidBySuffix2);
    }

    public void insertBlogDetailDepenadUidByDate(String startDate, String endDate) {
        processMblogFromUidDetailDataByDate(startDate, endDate, this::insertBlogDepenadUidBySuffix);
    }

    public void insertBlogDepenadUidBySuffix(String tableSuffix){
        int pageSize = 1000;
        long startTime = System.currentTimeMillis();
        log.info("开始处理mblog_from_uid_{}", tableSuffix);
        int tableExist = mblogFromUidMapper.isTableExist(tableSuffix);
        if (tableExist < 1){
            log.warn("表mblog_from_uid_{}不存在", tableSuffix);
            return;
        }
        long maxCount = mblogFromUidMapper.count(tableSuffix);
        log.info("统计mblog_from_uid_{}的总数为{}, 耗时{}ms", tableSuffix, maxCount,
                System.currentTimeMillis() - startTime);
        int count = 0;
        int pageNum = 1;
        maxCount = 10000;
        while (count < maxCount) {
            List<MblogFromUid> blogList = mblogFromUidMapper.findAll(tableSuffix, pageNum, pageSize);
            blogList.forEach(mblogFromUid -> {
                midUidTextService.insertByMblogFromUid(mblogFromUid);
            });
            count += blogList.size();
            pageNum++;
            log.info("mblog_from_uid_{}已处理:{}/{}, 已耗时{}ms", tableSuffix, count, maxCount,
                    System.currentTimeMillis() - startTime);
        }
        log.info("mblog_from_uid_{}已处理完成，实际生成{}条数据, 共耗时{}ms", tableSuffix, count,
                System.currentTimeMillis() - startTime);
    }

    public void insertBlogDepenadUidBySuffix2(String tableSuffix){
        int pageSize = 1000;
        long startTime = System.currentTimeMillis();
        log.info("开始处理mblog_from_uid_{}", tableSuffix);
        int tableExist = mblogFromUidMapper.isTableExist(tableSuffix);
        if (tableExist < 1){
            log.warn("表mblog_from_uid_{}不存在", tableSuffix);
            return;
        }
        long maxCount = mblogFromUidMapper.count(tableSuffix);
        log.info("统计mblog_from_uid_{}的总数为{}, 耗时{}ms", tableSuffix, maxCount,
                System.currentTimeMillis() - startTime);
        final int[] count = {0};
        int pageNum = 1;
        maxCount = 10000;
        Map<Long, List<MidUidText>> map = new HashMap<>();
        while (count[0] < maxCount) {
            List<MblogFromUid> blogList = mblogFromUidMapper.findAll(tableSuffix, pageNum, pageSize);
            blogList.forEach(mblogFromUid -> {
                String uid = mblogFromUid.getUid();
                Long uidLong = Long.parseLong(uid);

                if (map.containsKey(uidLong)) {
                    List<MidUidText> list = map.get(uidLong);
                    list.add(midUidTextService.parse(mblogFromUid));
                } else {
                    List<MidUidText> list = new ArrayList<>();
                    list.add(midUidTextService.parse(mblogFromUid));
                    map.put(uidLong, list);
                }

            });
            count[0] += blogList.size();
            pageNum++;
            log.info("mblog_from_uid_{}已处理:{}/{}, 已耗时{}ms", tableSuffix, count[0], maxCount,
                    System.currentTimeMillis() - startTime);
        }
        final long[] rowCount = {0};
        map.forEach((uid, mblogFromUidList) -> {
            rowCount[0] += midUidTextService.insertAll(uid, mblogFromUidList);
//            mblogFromUidList.forEach(mblogFromUid -> {
//                midUidTextService.insertByMblogFromUid(mblogFromUid);
//            });
        });
        log.info("mblog_from_uid_{}已处理完成，实际生成{}条数据, 共耗时{}ms", tableSuffix, rowCount[0],
                System.currentTimeMillis() - startTime);
        map.clear();
    }


    public void genBlogFileDepenadUidBySuffix(String tableSuffix) {
        int pageSize = 100000;
        long maxCount;
        long startTime = System.currentTimeMillis();
        log.info("开始处理mblog_from_uid_{}", tableSuffix);
        try {
            maxCount = mblogFromUidMapper.count(tableSuffix);
        } catch (InvalidDataAccessResourceUsageException e) {
            processDatabaseException(e, tableSuffix);
            return;
        }
        log.info("统计mblog_from_uid_{}的总数为{}, 耗时{}ms", tableSuffix, maxCount,
                System.currentTimeMillis() - startTime);
        int count = 0;
        int pageNum = 1;
        int uidLength = 10;
        Map<String, List<MblogFromUid>> map = new HashMap<>();
        while (count < maxCount) {
            List<MblogFromUid> blogList = mblogFromUidMapper.findAll(tableSuffix, pageNum, pageSize);
            blogList.forEach(mblogFromUid -> {
                if (!MID_SET.add(mblogFromUid.getMid())){
                    return;
                }
                String uid = mblogFromUid.getUid();

                if (map.containsKey(uid)) {
                    List<MblogFromUid> list = map.get(uid);
                    list.add(mblogFromUid);
                } else {
                    List<MblogFromUid> list = new ArrayList<>();
                    list.add(mblogFromUid);
                    map.put(uid, list);
                }
            });
            count += blogList.size();
            pageNum++;
            log.info("mblog_from_uid_{}已处理:{}/{}, 已耗时{}ms", tableSuffix, count, maxCount,
                    System.currentTimeMillis() - startTime);
        }
        int uidSize = map.size();
        final int[] uidCount = {0};
        map.forEach((uid, mblogFromUidList) -> {
            String uidPad = StringUtils.leftPad(uid, uidLength, "0");
            StringBuilder sb = new StringBuilder(mblogDataPath);
            for (int j = 0; j < uidLength; j += 2) {
                sb.append(File.separator).append(StringUtils.substring(uidPad, j, j + 2));
            }
            String resultPath = sb.toString();
            File resultDir = new File(resultPath);
            resultDir.mkdirs();
            Jedis jedis = RedisLock.REDIS_LOCK.getJedis();
            String uuid = UUID.randomUUID().toString();
            try {
                if (RedisLock.REDIS_LOCK.lock(jedis, uid, uuid)) {
                    String filePathBlog = resultPath + File.separator + "mblog.csv";
                    String filePathRetweet = resultPath + File.separator + "retweet.csv";
                    File fileBlog = new File(filePathBlog);
                    File fileRetweet = new File(filePathRetweet);
                    boolean appendBlog = fileBlog.exists();
                    boolean appendRetweet = fileRetweet.exists();
                    try (CSVPrinter blogWriter = CsvFileHelper.writer(fileBlog, HEADER_BLOG, appendBlog);
                         CSVPrinter retweetWriter = CsvFileHelper.writer(filePathRetweet, HEADER_BLOG, appendRetweet)) {
                        for (MblogFromUid mblogFromUid : mblogFromUidList) {
                            if (StringUtils.isNotBlank(mblogFromUid.getRetweetedMid())) {
                                blogWriter.printRecord(uid, mblogFromUid.getMid(), mblogFromUid.getText(),
                                        mblogFromUid.getCreateTime(), mblogFromUid.getAttitudesCount(),
                                        mblogFromUid.getCommentsCount(), mblogFromUid.getRepostsCount());
                            } else {
                                retweetWriter.printRecord(uid, mblogFromUid.getMid(), mblogFromUid.getText(),
                                        mblogFromUid.getCreateTime(), mblogFromUid.getAttitudesCount(),
                                        mblogFromUid.getCommentsCount(), mblogFromUid.getRepostsCount());
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                RedisLock.REDIS_LOCK.releaseDistributedLock(jedis, uid, uuid);
                uidCount[0]++;
            }
            if (uidCount[0] % 10000 == 0) {
                log.info("mblog_from_uid_{}已处理uid:{}/{}, 共耗时{}ms", tableSuffix, uidCount[0], uidSize,
                        System.currentTimeMillis() - startTime);
            }
        });
        map.clear();
        log.info("mblog_from_uid_{}已处理完成，实际生成{}条数据, 共耗时{}ms", tableSuffix, count,
                System.currentTimeMillis() - startTime);
    }

    private void writeBlog(MblogFromUid mblogFromUid, String resultPath) {
        Jedis jedis = RedisLock.REDIS_LOCK.getJedis();
        String uuid = UUID.randomUUID().toString();
        String uid = mblogFromUid.getUid();
        try {
            if (RedisLock.REDIS_LOCK.lock(jedis, uid, uuid)) {
                long startTime = System.currentTimeMillis();
                String fileName;
                if (StringUtils.isBlank(mblogFromUid.getRetweetedMid())) {
                    fileName = "mblog.csv";
                } else {
                    fileName = "retweet.csv";
                }
                File resultFile = new File(resultPath + File.separator + fileName);
                boolean append = resultFile.exists();
                try (CSVPrinter csvPrinter = CsvFileHelper.writer(resultFile, HEADER_BLOG, append)) {
                    csvPrinter.printRecord(uid, mblogFromUid.getMid(), mblogFromUid.getText(),
                            mblogFromUid.getCreateTime(), mblogFromUid.getAttitudesCount(),
                            mblogFromUid.getCommentsCount(), mblogFromUid.getRepostsCount());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                log.info("写入{}完成, 耗时{}ms", resultPath + File.separator + fileName,
                        System.currentTimeMillis() - startTime);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            RedisLock.REDIS_LOCK.releaseDistributedLock(jedis, uid, uuid);
        }
    }

//
//    public void genBlogFileDepenadUidBySuffix(String tableSuffix) {
//        int pageSize = 1000;
//        long maxCount;
//        long startTime = System.currentTimeMillis();
//        log.info("开始处理mblog_from_uid_{}", tableSuffix);
//        try {
//            maxCount = mblogFromUidMapper.count(tableSuffix);
//        } catch (InvalidDataAccessResourceUsageException e) {
//            processDatabaseException(e, tableSuffix);
//            return;
//        }
//        log.info("统计mblog_from_uid_{}的总数为{}, 耗时{}ms", tableSuffix, maxCount,
//                System.currentTimeMillis() - startTime);
//        int count = 0;
//        int pageNum = 1;
//        int uidLength = 10;
//        while (count < maxCount) {
//            List<MblogFromUid> blogList = mblogFromUidMapper.findAll(tableSuffix, pageNum, pageSize);
//            log.info("读取mblog_from_uid_{}的数据{}行, 耗时{}ms", tableSuffix, pageSize,
//                    System.currentTimeMillis() - startTime);
//            blogList.forEach(mblogFromUid -> {
//                String uid = mblogFromUid.getUid();
//                String uidPad = StringUtils.leftPad(uid, uidLength, "0");
//
//                StringBuilder sb = new StringBuilder(mblogDataPath);
//                for (int j = 0; j < uidLength; j += 2) {
//                    sb.append(File.separator).append(StringUtils.substring(uidPad, j, j + 2));
//                }
//                String resultPath = sb.toString();
//                File resultDir = new File(resultPath);
//                resultDir.mkdirs();
//                writeBlog(mblogFromUid, resultPath);
//            });
//            count += blogList.size();
//            pageNum++;
//            log.info("mblog_from_uid_{}已处理:{}/{}, 已耗时{}ms", tableSuffix, count, maxCount,
//                    System.currentTimeMillis() - startTime);
//        }
//        log.info("mblog_from_uid_{}已处理完成，实际生成{}条数据, 共耗时{}ms", tableSuffix, count,
//                System.currentTimeMillis() - startTime);
//    }
//
//    private void writeBlog(MblogFromUid mblogFromUid, String resultPath) {
//        Jedis jedis = RedisLock.REDIS_LOCK.getJedis();
//        String uuid = UUID.randomUUID().toString();
//        String uid = mblogFromUid.getUid();
//        try {
//            if (RedisLock.REDIS_LOCK.lock(jedis, uid, uuid)) {
//                long startTime = System.currentTimeMillis();
//                String fileName;
//                if (StringUtils.isBlank(mblogFromUid.getRetweetedMid())) {
//                    fileName = "mblog.csv";
//                } else {
//                    fileName = "retweet.csv";
//                }
//                File resultFile = new File(resultPath + File.separator + fileName);
//                boolean append = resultFile.exists();
//                try (CSVPrinter csvPrinter = CsvFileHelper.writer(resultFile, HEADER_BLOG, append)) {
//                    csvPrinter.printRecord(uid, mblogFromUid.getMid(), mblogFromUid.getText(),
//                            mblogFromUid.getCreateTime(), mblogFromUid.getAttitudesCount(),
//                            mblogFromUid.getCommentsCount(), mblogFromUid.getRepostsCount());
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                log.info("写入{}完成, 耗时{}ms", resultPath + File.separator + fileName,
//                        System.currentTimeMillis() - startTime);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            RedisLock.REDIS_LOCK.releaseDistributedLock(jedis, uid, uuid);
//        }
//    }

    public void genRetweetFileByDate(String resultPath, String startDate, String endDate) {

        // TODO 临时
        if (isUserTypeFromFile) {
            UserTypeUtils.initUserSet();
        }

        List<String> dateList = DateTimeUtils.getDateList(startDate, endDate);
        int indexSize = 10;
        CountDownLatch countDownLatch = new CountDownLatch(dateList.size() * indexSize);
        for (String date : dateList) {
            for (int i = 0; i < indexSize; i++) {
                final String suffix = date + CommonConstants.UNDERLINE + i;
                taskExecutor.execute(() -> {
                    try {
                        genRetweetFile(resultPath, suffix);
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void genRetweetDetailFileByDate(String resultPath, String startDate, String endDate) {

        if (isUserTypeFromFile) {
            UserTypeUtils.initUserSet();
        }

        List<String> dateList = DateTimeUtils.getDateList(startDate, endDate);
        CountDownLatch countDownLatch = new CountDownLatch(dateList.size());
        for (String date : dateList) {
            final String suffix = "detail_" + date;
            taskExecutor.execute(() -> {
                try {
                    genRetweetFile(resultPath, suffix);
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
    }

    public void genRetweetFile(String resultPath, String tableSuffix) {
        int pageSize = 100000;
        long maxCount;
        long startTime = System.currentTimeMillis();
        log.info("开始处理mblog_from_uid_{}", tableSuffix);
        try {
            maxCount = mblogFromUidMapper.countRetweet(tableSuffix);
        } catch (InvalidDataAccessResourceUsageException e) {
            processDatabaseException(e, tableSuffix);
            return;
        }
        log.info("统计mblog_from_uid_{}的转发数为{}, 耗时{}ms", tableSuffix, maxCount,
                System.currentTimeMillis() - startTime);
        int count = 0;
        int pageNum = 1;
        Map<String, List<RetweetDTO>> map = new HashMap<>(6);

        while (count < maxCount) {
            List<MblogFromUid> blogRetweetList = mblogFromUidMapper.findAllRetweet(tableSuffix, pageNum, pageSize);
            if (isUserTypeFromFile) {
                wrapRetweetData2(blogRetweetList, map);
            } else {
                wrapRetweetData(blogRetweetList, map);
            }
            count += blogRetweetList.size();
            pageNum++;
            log.info("mblog_from_uid_{}已处理:{}/{}, 已耗时{}ms", tableSuffix, count, maxCount,
                    System.currentTimeMillis() - startTime);
        }
        File fileDic = new File(resultPath + File.separator + RETWEET_FILE_DIC);
        fileDic.mkdirs();
        final int[] resultCount = {0};
        map.forEach((key, retweetList) -> {
            resultCount[0] += retweetList.size();
            String retweetFileDicName = fileDic.getPath() + File.separator + key;
            File retweetFileDic = new File(retweetFileDicName);
            retweetFileDic.mkdir();
            String filePath = retweetFileDicName + File.separator + key + CommonConstants.UNDERLINE + tableSuffix
                    + FileSuffixConstants.CSV;
            try {
                writeToCsvFile(retweetList, filePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        log.info("mblog_from_uid_{}已处理完成，实际生成{}条边数据, 共耗时{}ms", tableSuffix, resultCount[0],
                System.currentTimeMillis() - startTime);
    }

    /**
     * @param dataPath   retweet文件夹所在目录
     * @param resultPath 合并结果生成目录
     */
    public void mergeRetweetFiles(String dataPath, String resultPath) {
        File retweetFile = new File(dataPath + File.separator + RETWEET_FILE_DIC);
        if (!retweetFile.exists()) {
            return;
        }
        String[] fileList = retweetFile.list();
        if (fileList == null || fileList.length == 0) {
            return;
        }
        CountDownLatch mergeCountDownLatch = new CountDownLatch(fileList.length);
        for (String fileName : fileList) {
            taskExecutor.execute(() -> {
                try {
                    mergeFile(retweetFile.getPath() + File.separator + fileName, resultPath, fileName);
                } finally {
                    mergeCountDownLatch.countDown();
                }
            });
        }
        try {
            mergeCountDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据SSDB的userInfo判断用户类型
     *
     * @param retweetList
     * @param map
     */
    private void wrapRetweetData(List<MblogFromUid> retweetList, Map<String, List<RetweetDTO>> map) {
        retweetList.forEach(mblogFromUid -> {

            String fromUid = mblogFromUid.getUid();
            String mid;
            String pid = mblogFromUid.getPid();
            if (StringUtils.isNotBlank(pid)) {
                mid = pid;
            } else {
                mid = mblogFromUid.getRetweetedMid();
            }

            UserInfo fromUserInfo = redisDataManager.getUserInfoViaCache(fromUid);
            if (fromUserInfo == null || !fromUserInfo.isCoreUser()) {
                return;
            }
            String toUid = redisDataManager.getUidByMid(mid);
            if (StringUtils.isBlank(toUid)) {
                return;
            }
            UserInfo toUserInfo = redisDataManager.getUserInfoViaCache(toUid);
            if (toUserInfo == null || !toUserInfo.isCoreUser()) {
                return;
            }
            String fromUserType = UserInfoUtils.getUserType(fromUserInfo);
            String toUserType = UserInfoUtils.getUserType(toUserInfo);

            if (StringUtils.isNotBlank(fromUserType) && StringUtils.isNotBlank(toUserType)) {
                String relationship = fromUserType + RELATION_RETWEET + toUserType;
                RetweetDTO retweetDTO = new RetweetDTO(fromUid, toUid);
                retweetDTO.setMid(mid);
                retweetDTO.setCreateTime(mblogFromUid.getCreateTime());
                if (map.containsKey(relationship)) {
                    map.get(relationship).add(retweetDTO);
                } else {
                    List<RetweetDTO> list = new ArrayList<>();
                    list.add(retweetDTO);
                    map.put(relationship, list);
                }
            }
        });
    }

    /**
     * 根据内存set判断用户类型
     *
     * @param retweetList
     * @param map
     */
    private void wrapRetweetData2(List<MblogFromUid> retweetList, Map<String, List<RetweetDTO>> map) {
        retweetList.forEach(mblogFromUid -> {

            String fromUid = mblogFromUid.getUid();
            Long fromUidLong = Long.valueOf(fromUid);
            if (!UserTypeUtils.isCoreUser(fromUidLong)) {
                return;
            }

            String mid;
            String pid = mblogFromUid.getPid();
            if (StringUtils.isNotBlank(pid)) {
                mid = pid;
            } else {
                mid = mblogFromUid.getRetweetedMid();
            }
            String toUid = redisDataManager.getUidByMid(mid);
            if (StringUtils.isBlank(toUid)) {
                return;
            }
            Long toUidLong = Long.valueOf(toUid);
            if (!UserTypeUtils.isCoreUser(toUidLong)) {
                return;
            }
            String fromUserType = UserTypeUtils.getUserType(fromUidLong);
            String toUserType = UserTypeUtils.getUserType(toUidLong);

            if (StringUtils.isNotBlank(fromUserType) && StringUtils.isNotBlank(toUserType)) {
                String relationship = fromUserType + RELATION_RETWEET + toUserType;
                RetweetDTO retweetDTO = new RetweetDTO(fromUid, toUid);
                retweetDTO.setMid(mid);
                retweetDTO.setCreateTime(mblogFromUid.getCreateTime());
                if (map.containsKey(relationship)) {
                    map.get(relationship).add(retweetDTO);
                } else {
                    List<RetweetDTO> list = new ArrayList<>();
                    list.add(retweetDTO);
                    map.put(relationship, list);
                }
            }
        });
    }

    private void writeToCsvFile(List<RetweetDTO> content, String filePath) throws IOException {
        try (CSVPrinter printer = CsvFileHelper.writer(filePath, HEADER_RETWEET)) {
            for (RetweetDTO retweetDTO : content) {
                printer.printRecord(retweetDTO.getFromUid(), retweetDTO.getToUid(), retweetDTO.getMid(),
                        retweetDTO.getCreateTime());
            }
        }
    }

    private void mergeFile(String oriFileDir, String resultDirPath, String resultFileName) {
        File file = new File(oriFileDir);
        if (file.isDirectory()) {
            String[] fileNames = file.list();
            if (fileNames == null || fileNames.length == 0) {
                return;
            }
            List<String> filePathList = new ArrayList<>();
            for (String fileName : fileNames) {
                filePathList.add(oriFileDir + File.separator + fileName);
            }
            CsvFile csvFile = CsvFile.build(resultDirPath, resultFileName).withHeader(HEADER_RETWEET);
            csvFileManager.mergeFile(filePathList, csvFile);
        }
    }

    private void processDatabaseException(RuntimeException e, String tableSuffix) {
        String message = e.getMessage();
        if (StringUtils.containsIgnoreCase(message, "Table")
                && StringUtils.containsIgnoreCase(message, "doesn't exist")) {
            log.warn("表mblog_from_uid_{}不存在", tableSuffix);
        } else {
            log.error("处理表mblog_from_uid_" + tableSuffix + "出错", e);
        }
    }

    private void processMblogFromUidDataByDate(String startDate, String endDate, Consumer<String> consumer){
        List<String> dateList = DateTimeUtils.getDateList(startDate, endDate);
        int indexSize = 10;
        CountDownLatch countDownLatch = new CountDownLatch(dateList.size() * indexSize);
        for (String date : dateList) {
            for (int i = 0; i < indexSize; i++) {
                String tableSuffix = date + CommonConstants.UNDERLINE + i;
                taskExecutor.execute(() -> {
                    try {
                        consumer.accept(tableSuffix);
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void processMblogFromUidDetailDataByDate(String startDate, String endDate, Consumer<String> consumer){
        List<String> dateList = DateTimeUtils.getDateList(startDate, endDate);
        CountDownLatch countDownLatch = new CountDownLatch(dateList.size());
        for (String date : dateList) {
            String tableSuffix = "detail" + CommonConstants.UNDERLINE + date;
            taskExecutor.execute(() -> {
                try {
                    consumer.accept(tableSuffix);
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
    }
}
