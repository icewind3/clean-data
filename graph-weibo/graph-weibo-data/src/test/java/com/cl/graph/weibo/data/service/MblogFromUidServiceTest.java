package com.cl.graph.weibo.data.service;

import com.cl.graph.weibo.core.constant.CommonConstants;
import com.cl.graph.weibo.core.constant.FileSuffixConstants;
import com.cl.graph.weibo.core.exception.ServiceException;
import com.cl.graph.weibo.data.BaseSpringBootTest;
import com.cl.graph.weibo.data.entity.MblogFromUid;
import com.cl.graph.weibo.data.manager.RedisDataManager;
import com.cl.graph.weibo.data.mapper.marketing.MblogFromUidMapper;
import com.cl.graph.weibo.data.util.CsvFileHelper;
import com.cl.graph.weibo.data.util.UserTypeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/7/17
 */
@Slf4j
public class MblogFromUidServiceTest extends BaseSpringBootTest {

    @Autowired
    private MblogFromUidService mblogFromUidService;

    @Autowired
    private RedisDataManager redisDataManager;

    @Resource
    private MblogFromUidMapper mblogFromUidMapper;

    @Test
    public void genRetweetFile() {
        String resultFilePath = "C:/Users/cl32/Downloads/微博大图";
        String tableSuffix = "detail_2019031722";
        mblogFromUidService.genRetweetFile(resultFilePath, tableSuffix);
    }

    @Test
    public void test() {
        List<MblogFromUid> allRetweet = mblogFromUidMapper.findAllRetweet("20190718_0", 1, 5000);
        System.out.println(allRetweet);
    }

    @Test
    public void processOneFile() {
        UserTypeUtils.initUserSet();
        String[] readHeader = {"mid", "uid", "reposts_count", "comments_count", "attitudes_count", "text", "pid", "retweeted_mid", "created_at"};
        String[] retweetHeader = {"from", "to", "mid", "createTime"};
        String RELATION_RETWEET = "_retweet_";
        String filePath = "C:/Users/cl32/Desktop/mblog_from_uid2.csv";
        String resultPath = "C:/Users/cl32/Desktop/edge";
        int count = 0;
        int edgeCount = 0;
        long startTime = System.currentTimeMillis();
        log.info("开始处理mblog_from_uid");
        try (CSVParser csvParser = CsvFileHelper.reader(filePath, readHeader, true)) {
            for (CSVRecord csvRecord : csvParser) {
                count++;
                String retweetedMid = csvRecord.get("retweeted_mid");
                if (StringUtils.isBlank(retweetedMid)) {
                    continue;
                }
                String fromUid = csvRecord.get("uid");
                Long fromUidLong = Long.valueOf(fromUid);
                if (!UserTypeUtils.isCoreUser(fromUidLong)) {
                    continue;
                }
                String mid;
                String pid = csvRecord.get("pid");
                if (StringUtils.isNotBlank(pid)) {
                    mid = pid;
                } else {
                    mid = retweetedMid;
                }
                String toUid = redisDataManager.getUidByMid(mid);
                if (StringUtils.isBlank(toUid)) {
                    continue;
                }
                Long toUidLong = Long.valueOf(toUid);
                if (!UserTypeUtils.isCoreUser(toUidLong)) {
                    continue;
                }
                String fromUserType = UserTypeUtils.getUserType(fromUidLong);
                String toUserType = UserTypeUtils.getUserType(toUidLong);
                if (StringUtils.isNotBlank(fromUserType) && StringUtils.isNotBlank(toUserType)) {
                    String relationship = fromUserType + RELATION_RETWEET + toUserType;
                    File dir = new File(resultPath + File.separator + relationship);
                    dir.mkdirs();
                    File resultFile = new File(dir.getPath() + File.separator + relationship
                            + CommonConstants.UNDERLINE + "0" + FileSuffixConstants.CSV);
                    boolean append = resultFile.exists();
                    try (CSVPrinter csvPrinter = CsvFileHelper.writer(resultFile, retweetHeader, append)) {
                        csvPrinter.printRecord(fromUid, toUid, mid, csvRecord.get("created_at"));
                        edgeCount++;
                    }
                }
                if (edgeCount % 10000 == 0) {
                    log.info("mblog_from_uid已处理:{}, 已生成{}条边, 已耗时{}ms", count, edgeCount,
                            System.currentTimeMillis() - startTime);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("mblog_from_uid.csv已处理完成，共处理{}数据，实际生成{}条边数据, 共耗时{}ms", count, edgeCount,
                System.currentTimeMillis() - startTime);
    }

    @Test
    public void processOneFileToALLBlog() {
        String[] readHeader = {"mid", "uid", "reposts_count", "comments_count", "attitudes_count", "text", "pid", "retweeted_mid", "created_at"};
        String[] blogHeader = {"uid", "mid", "text", "created_at"};
        String filePath = "C:/Users/cl32/Desktop/mblog_from_uid.csv";
        String resultPath = "C:/Users/cl32/Desktop/mblog2";
        String fileName = "mblog_from_uid";
        int count = 0;
        int index = 0;
        int segmentSize = 6000000;
        long startTime = System.currentTimeMillis();
        log.info("开始处理mblog_from_uid");
        CSVPrinter csvPrinter = null;
        try {
            File newFile = null;
            try (CSVParser csvParser = CsvFileHelper.reader(filePath, readHeader, true)) {
                for (CSVRecord record : csvParser) {
                    if (csvPrinter == null || count >= segmentSize) {
                        if (csvPrinter != null) {
                            if (log.isInfoEnabled()) {
                                log.info("分割写入文件{}完成，耗时{}ms", newFile.getPath(), System.currentTimeMillis() - startTime);
                            }
                            csvPrinter.close();
                        }
                        count = 0;
                        newFile = new File(resultPath + File.separator + fileName + index + FileSuffixConstants.CSV);
                        index++;
                        csvPrinter = CsvFileHelper.writer(newFile, blogHeader);
                        if (log.isInfoEnabled()) {
                            startTime = System.currentTimeMillis();
                            log.info("开始写入分割文件{}", newFile.getPath());
                        }
                    }
                    csvPrinter.printRecord(record.get("uid"),record.get("mid"),record.get("text"), record.get("created_at"));
                    count++;
                }
            } catch (IOException e) {
                throw new ServiceException(e);
            }
        } finally {
            if (csvPrinter != null) {
                try {
                    csvPrinter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        log.info("mblog_from_uid.csv已处理完成，共处理{}数据，共耗时{}ms", count, System.currentTimeMillis() - startTime);
    }

}