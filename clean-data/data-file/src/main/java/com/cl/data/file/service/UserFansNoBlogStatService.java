package com.cl.data.file.service;

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

    public void computeUserFansNoBlogStat(String hasBlogUidPath, String uidFilePath, String resultPath) throws IOException {
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
        Set<Long> uidSet = new HashSet<>();
        try (CSVParser csvParser = CsvFileHelper.reader(hasBlogUidPath)) {
            for (CSVRecord record : csvParser) {
                String uid = record.get(0);
                uidSet.add(Long.parseLong(uid));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("加载有博文用户列表完成, size = {}，耗时{}ms", uidSet.size(), System.currentTimeMillis() - startTime);

        try (CSVPrinter csvPrinter = CsvFileHelper.writer(resultPath + File.separator + "UserFansStat.csv");
             CSVPrinter uidWriter = CsvFileHelper.writer(resultPath + File.separator + "NoBlogFans.csv")) {
            list.forEach(file -> {
                String uid = file.getName();

                int fansCount =  0;
                int noBlogCount = 0;
                try (CSVParser csvParser = CsvFileHelper.reader(file)) {
                    for (CSVRecord record : csvParser) {
                        fansCount++;
                        Long fansUid = Long.parseLong(record.get(0));
                        if (!uidSet.contains(fansUid)) {
                            noBlogCount++;
                            uidWriter.printRecord(fansUid);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
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


    public void filterHasBlogUid(String filePath, String resultPath) throws IOException {
        File uidFile = new File(filePath);
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
        final int allFileCount = list.size();
        Set<String> uidSet = new HashSet<>();
        log.info("开始过滤有博文的用户, path = {}，总共{}个文件", filePath, allFileCount);
        try (CSVPrinter uidWriter = CsvFileHelper.writer(resultPath)) {
            list.forEach(file -> {
                try (CSVParser csvParser = CsvFileHelper.reader(file, new String[]{}, true)) {
                    for (CSVRecord record : csvParser) {
                        String uid = record.get(0);
                        if (uidSet.add(uid)) {
                            uidWriter.printRecord(uid);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                log.info("文件{}已完成，耗时{}ms", file.getPath(), System.currentTimeMillis() - startTime);
            });
        }
        log.info("过滤有博文的用户, uidPath={}，总共{}个文件，耗时{}ms", filePath, allFileCount,
            System.currentTimeMillis() - startTime);
    }

    public void mergeUid(String filePath, String resultPath) throws IOException {
        File uidFile = new File(filePath);
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
        Set<String> uidSet = new HashSet<>();
        log.info("开始合并用户, path = {}，总共{}个文件", filePath, list.size());
        try (CSVPrinter uidWriter = CsvFileHelper.writer(resultPath)) {
            list.forEach(file -> {
                try (CSVParser csvParser = CsvFileHelper.reader(file)) {
                    for (CSVRecord record : csvParser) {
                        String uid = record.get(0);
                        if (uidSet.add(uid)) {
                            uidWriter.printRecord(uid);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                log.info("文件{}已完成，耗时{}ms", file.getPath(), System.currentTimeMillis() - startTime);
            });
        }
        log.info("合并用户完成, 耗时{}ms", System.currentTimeMillis() - startTime);
    }
}
