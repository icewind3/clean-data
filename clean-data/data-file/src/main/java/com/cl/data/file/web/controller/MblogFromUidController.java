package com.cl.data.file.web.controller;

import com.cl.graph.weibo.core.util.CsvFileHelper;
import com.cl.graph.weibo.core.web.ResponseResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;

/**
 * @author yejianyu
 * @date 2019/10/18
 */
@Slf4j
@RestController
@RequestMapping(value = "/blog")
public class MblogFromUidController {

    private static final String[] HEADER = {"mid", "uid", "reposts_count", "comments_count", "attitudes_count", "text",
        "retweeted_mid", "created_at"};

    @RequestMapping(value = "/filterIsRetweeted")
    public ResponseResult filterIsRetweeted(@RequestParam String filePath, @RequestParam String resultPath) {
        File file = new File(filePath);
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null && files.length > 0) {
                for (File file1 : files) {
                    doFilterIsRetweeted(file1, resultPath);
                }
            }
        } else {
            doFilterIsRetweeted(file, resultPath);
        }

        return ResponseResult.SUCCESS;
    }

    private void doFilterIsRetweeted(File file, String resultPath){
        long startTime = System.currentTimeMillis();
        log.info("开始处理文件{}", file.getPath());
        try (CSVParser csvParser = CsvFileHelper.reader(file, HEADER, true);
             CSVPrinter csvPrinter = CsvFileHelper.writer(resultPath, true)) {
            for (CSVRecord record : csvParser) {
                String retweetedMid = record.get("retweeted_mid");
                String isRetweeted = "0";
                if (StringUtils.isNotBlank(retweetedMid)) {
                    isRetweeted = "1";
                }
                csvPrinter.printRecord(record.get("uid"), record.get("mid"), isRetweeted);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("文件{}处理完成，耗时{}ms", file.getName(), System.currentTimeMillis() - startTime);
    }
}
