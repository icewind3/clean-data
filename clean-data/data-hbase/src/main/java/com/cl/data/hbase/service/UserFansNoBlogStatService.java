package com.cl.data.hbase.service;

import com.cl.graph.weibo.core.util.CsvFileHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author yejianyu
 * @date 2019/10/14
 */
@Service
@Slf4j
public class UserFansNoBlogStatService {

    @Resource
    private UidMidBlogHbaseService uidMidBlogHbaseService;

    public void computeUserFansNoBlogStat(String uidFilePath, String resultPath) throws IOException {
        File uidFile = new File(uidFilePath);
        List<File> list = new ArrayList<>();
        if (uidFile.isDirectory()) {
            File[] files = uidFile.listFiles();
            if (files != null) {
                Collections.addAll(list, files);
            }
        } else {
            list.add(uidFile);
        }
        long startTime = System.currentTimeMillis();
        AtomicInteger count = new AtomicInteger();
        final int allUserCount = list.size();
        log.info("开始统计KOL僵尸粉, uidPath = {}，总共{}个文件", uidFilePath, allUserCount);
        try (CSVPrinter csvPrinter = CsvFileHelper.writer(resultPath + File.separator + "UserFansStat.csv");
             CSVPrinter uidWriter = CsvFileHelper.writer(resultPath + File.separator + "NoBlogFans.csv")) {
            list.forEach(file -> {
                String uid = file.getName();

                Set<String> fansUidSet = new HashSet<>();
                try (CSVParser csvParser = CsvFileHelper.reader(file)) {
                    for (CSVRecord record : csvParser) {
                        String fansUid = record.get(0);
                        fansUidSet.add(fansUid);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                int fansCount =  fansUidSet.size();
                try {
                    Set<String> noBlogUserSet = uidMidBlogHbaseService.getNoBlogUser(fansUidSet);
                    for (String noBlogUid : noBlogUserSet) {
                        uidWriter.printRecord(noBlogUid);
                    }
                    int noBlogCount = noBlogUserSet.size();
                    csvPrinter.printRecord(uid, fansCount, noBlogCount, (float) noBlogCount / fansCount);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                int i = count.incrementAndGet();
                if (i % 100 == 0) {
                    log.info("统计KOL僵尸粉进度 {}/{}，耗时{}ms", i, allUserCount, System.currentTimeMillis() - startTime);
                }
            });
        }
        log.info("统计KOL僵尸粉完成, uidPath={}，总共{}个文件，耗时{}ms", uidFilePath, list.size(),
            System.currentTimeMillis() - startTime);

    }
}
