package com.cl.data.hbase.service;

import com.cl.data.hbase.dto.BlogAnalysisResultDTO;
import com.cl.graph.weibo.core.util.CsvFileHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
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
public class WordSegmentationFileService {

    private static final String[] HEADER_RESULT_3_NEED = {"uid", "mid", "caizhi", "pingjia", "product_caizhi", "tiyangan",
            "xiangguan_function", "xingrong", "created_at", "weight"};
    private static final Set<String> COMMON_HEADER_SET = new HashSet<String>() {{
        add("uid");
        add("mid");
        add("created_at");
        add("weight");
    }};

    @Resource
    private WordSegmentationHbaseService wordSegmentationHbaseService;

    @Resource(name = "hbaseThreadPoolTaskExecutor")
    private ThreadPoolTaskExecutor hbaseThreadPoolTaskExecutor;

    public void processFileResult(String filePath, String[] header, String family) {
        processFile(filePath, file -> {
            processOneFile(file, header, family);
        });
    }

    public void processFileResult3(String filePath, String[] header, String family) {
        processFile(filePath, file -> {
            processOneFile(file, header, family, HEADER_RESULT_3_NEED);
        });
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

    private void processOneFile(File file, String[] header, String family) {
        processOneFile(file, header, family, null);
    }

    private void processOneFile(File file, String[] header, String family,  String[] needHeader) {
        log.info("开始处理分词结果文件{}", file.getPath());
        long startTime = System.currentTimeMillis();
        int count = 0;
        List<BlogAnalysisResultDTO> list = new ArrayList<>();
        try (CSVParser csvParser = CsvFileHelper.reader(file, header, true)) {
            for (CSVRecord record : csvParser) {
                String uid = record.get("uid");
                String mid = record.get("mid");
                String createdAt
                    = record.get("created_at");
                BlogAnalysisResultDTO blogAnalysisResult = new BlogAnalysisResultDTO();
                blogAnalysisResult.setUid(uid);
                blogAnalysisResult.setMid(mid);
                blogAnalysisResult.setCreatedAt(createdAt);
                if (needHeader == null){
                    needHeader = header;
                }
                blogAnalysisResult.setTypeWordMap(getTypeWordMap(needHeader, record));
                list.add(blogAnalysisResult);
                count++;
                if (count % 5000 == 0) {
                    wordSegmentationHbaseService.batchInsertBlogAnalysisResult(family, list);
                    list.clear();
                }
                if (count % 100000 == 0) {
                    log.info("处理分词结果, filePath={}, progress={}, time={}ms", file.getPath(), count,
                            System.currentTimeMillis() - startTime);
                }
            }
            if (list.size() > 0) {
                wordSegmentationHbaseService.batchInsertBlogAnalysisResult(family, list);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("分词结果处理完成, filePath={}, total={}, time={}ms", file.getPath(), count,
                System.currentTimeMillis() - startTime);
    }

    private Map<String, String> getTypeWordMap(String[] header, CSVRecord record) {
        Map<String, String> map = new HashMap<>();
        for (String key : header) {
            if (!COMMON_HEADER_SET.contains(key)) {
                String wordResult = record.get(key);
                map.put(key, wordResult);
            }
        }
        return map;
    }
}
