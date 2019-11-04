package com.cl.data.file.service;

import com.cl.graph.weibo.core.util.CsvFileHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author yejianyu
 * @date 2019/9/4
 */
@Slf4j
@Service
public class UidMidBlogPesgFileService {

    private static final String[] HEADER_PESG = {"uid", "mid", "app", "author", "book", "brand", "business_china",
        "business_group", "business_overseas", "chezaiyongpin", "city", "dianshi", "dongman", "duanpian", "ebook",
        "enterprise", "film", "jilupian", "music", "product", "qichebaoxian", "qichechangshang", "qichechexi",
        "qichechexing", "qichejibie", "qichemeirongchanpin", "qichepeizhi", "qichepinpai", "qicheqita",
        "qichezhengjian", "star_en", "star_zh", "zongyi", "created_at", "weight"};

    public void filterBlogPesgResult(String uidMidFilePath, String oriFilePath, String resultPath) {
        List<String> filePathList = getAllFilePath(oriFilePath);
        if (filePathList.size() == 0){
            return;
        }
        Set<String> midSet = initMidSet(uidMidFilePath);
        filePathList.forEach(s -> {
            filterOneFile(s, resultPath, midSet);
        });
        printMid(midSet);
    }

    private Set<String> initMidSet(String uidMidFilePath) {
        Set<String> midSet = new HashSet<>();
        try (CSVParser csvParser = CsvFileHelper.reader(uidMidFilePath)) {
            for (CSVRecord record : csvParser) {
                String mid = record.get(1);
                midSet.add(mid);
            }
            log.info("init mid_set finish mid_size={}", midSet.size());
        } catch (IOException e){
            e.printStackTrace();
        }
        return midSet;
    }

    private void printMid(Set<String> midSet) {
        try (CSVPrinter writer = CsvFileHelper.writer("mid.csv")) {
            midSet.forEach(mid -> {
                try {
                    writer.printRecord(mid);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            log.info("print mid_set finish mid_size={}", midSet.size());
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    private void filterOneFile(String filePath, String resultPath, Set<String> midSet) {
        long startTime = System.currentTimeMillis();
        log.info("START process file {}", filePath);
        File resultFile = new File(resultPath);
        boolean append = resultFile.exists();
        try (CSVParser csvParser = CsvFileHelper.reader(filePath, HEADER_PESG, true);
             CSVPrinter writer = CsvFileHelper.writer(resultPath, HEADER_PESG, append)) {
            for (CSVRecord record : csvParser) {
                String mid = record.get("mid");
                if (midSet.contains(mid)) {
                    writer.printRecord(record);
                    midSet.remove(mid);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("END process file {}, times={}ms", filePath, System.currentTimeMillis() - startTime);
    }

    private List<String> getAllFilePath(String filePath) {
        File oriFile = new File(filePath);
        if (oriFile.isDirectory()) {
            String[] files = oriFile.list();
            if (files == null || files.length == 0) {
                return Collections.emptyList();
            }
            List<String> fileList = new ArrayList<>();
            for (String file : files) {
                fileList.addAll(getAllFilePath(filePath + File.separator + file));
            }
            return fileList;
        }
        return Collections.singletonList(filePath);
    }

    public void filterBlogPesgResultByType(String uidMidFilePath, String oriFilePath, String resultPath) {
        List<String> filePathList = getAllFilePath(oriFilePath);
        if (filePathList.size() == 0){
            return;
        }
        Set<String> attitudeMidSet = initMidSet(uidMidFilePath + "/attitude/uid_mid_attitude_top50.csv");
        Set<String> commentMidSet = initMidSet(uidMidFilePath + "/comment/uid_mid_comment_top50.csv");
        Set<String> repostMidSet = initMidSet(uidMidFilePath + "/repost/uid_mid_repost_top50.csv");
        AtomicBoolean append = new AtomicBoolean(false);
        filePathList.forEach(filePath -> {
            long startTime = System.currentTimeMillis();
            log.info("START process file {}", filePath);
            try (CSVParser csvParser = CsvFileHelper.reader(filePath, HEADER_PESG, true);
                 CSVPrinter attitudeWriter = CsvFileHelper.writer(resultPath + "/attitude/uid_mid_blog_pesg_attitude_top50.csv", HEADER_PESG, append.get());
                 CSVPrinter commentWriter = CsvFileHelper.writer(resultPath + "/comment/uid_mid_blog_pesg_comment_top50.csv", HEADER_PESG, append.get());
                 CSVPrinter repostWriter = CsvFileHelper.writer(resultPath + "/repost/uid_mid_blog_pesg_repost_top50.csv", HEADER_PESG, append.get())) {
                for (CSVRecord record : csvParser) {
                    String mid = record.get("mid");
                    if (attitudeMidSet.contains(mid)) {
                        attitudeWriter.printRecord(record);
                    }
                    if (commentMidSet.contains(mid)) {
                        commentWriter.printRecord(record);
                    }
                    if (repostMidSet.contains(mid)) {
                        repostWriter.printRecord(record);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            log.info("END process file {}, times={}ms", filePath, System.currentTimeMillis() - startTime);
            append.set(true);
        });
    }


}
