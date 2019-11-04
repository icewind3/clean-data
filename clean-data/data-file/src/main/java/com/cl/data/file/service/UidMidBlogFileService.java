package com.cl.data.file.service;

import com.cl.data.file.util.BlogCleanUtils;
import com.cl.graph.weibo.core.constant.CommonConstants;
import com.cl.graph.weibo.core.constant.FileSuffixConstants;
import com.cl.graph.weibo.core.util.CsvFileHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yejianyu
 * @date 2019/9/4
 */
@Slf4j
@Service
public class UidMidBlogFileService {

    private static final String[] HEADER_MBLOG = {"mid", "uid", "reposts_count", "comments_count", "attitudes_count",
            "text", "retweeted_text", "created_at"};
    private static final String[] HEADER_MBLOG3 = {"mid", "uid", "reposts_count", "comments_count", "attitudes_count",
            "text", "retweeted_mid", "created_at"};
    private static final String[] HEADER_MBLOG2 = {"uid", "mid", "text","created_at"};
    private static final String[] HEADER_MBLOG_FROM_UID = {"mid","bid","uid","reposts_count","comments_count","attitudes_count","text","pics","source","duration","play_count","page_title","page_type","pid","retweeted_mid","retweeted_uid","created_at","order","timestamp"};
    private static final String[] HEADER_MBLOG_FROM_UID2 = {"mid","bid","uid","reposts_count","comments_count","attitudes_count","text", "retweeted_text", "pics","source","duration","play_count","page_title","page_type","pid","retweeted_mid","retweeted_uid","created_at","order","timestamp"};
    private static final String[] HEADER_UID_MID_BLOG = {"uid", "mid", "text", "retweeted_text", "created_at"};

    public void cleanToUidMidBlogFile(String filePath, String resultPath) {
        File file = new File(filePath);
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files == null || files.length == 0) {
                return;
            }
            for (File file1 : files) {
                cleanOneFileToUidMidBlogFile(file1, resultPath);
            }
        } else {
            cleanOneFileToUidMidBlogFile(file, resultPath);
        }
    }

    public void genMblogFromUidFile(String filePath, String resultPath) {
        File file = new File(filePath);
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files == null || files.length == 0) {
                return;
            }
            for (File file1 : files) {
                genOneMblogFromUidFile(file1, resultPath);
            }
        } else {
            genOneMblogFromUidFile(file, resultPath);
        }
    }

    private void cleanOneFileToUidMidBlogFile(File file, String resultPath) {
        File resultDir = new File(resultPath);
        resultDir.mkdirs();
        try (CSVParser csvParser = CsvFileHelper.reader(file, HEADER_MBLOG, true);
             CSVPrinter writer = CsvFileHelper.writer(resultPath + File.separator + file.getName(), HEADER_UID_MID_BLOG)) {
            for (CSVRecord record : csvParser) {
                String text = BlogCleanUtils.cleanBlog(record.get("text"));
                String retweetedText = BlogCleanUtils.cleanBlog(record.get("retweeted_text"));
//                String retweetedText = "";
                writer.printRecord(record.get("uid"), record.get("mid"), text, retweetedText, record.get("created_at"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void genOneMblogFromUidFile(File file, String resultPath) {
        File resultDir = new File(resultPath);
        resultDir.mkdirs();
        try (CSVParser csvParser = CsvFileHelper.reader(file, HEADER_MBLOG_FROM_UID, true);
             CSVPrinter writer = CsvFileHelper.writer(resultPath + File.separator + file.getName(), HEADER_MBLOG3)) {
            for (CSVRecord record : csvParser) {
//                writer.printRecord(record.get("mid"), record.get("uid"), record.get("reposts_count"),
//                    record.get("comments_count"),record.get("attitudes_count"), record.get("text"),
//                    record.get("retweeted_text"), record.get("created_at"));


                writer.printRecord(record.get("mid"), record.get("uid"), record.get("reposts_count"),
                    record.get("comments_count"),record.get("attitudes_count"), record.get("text"),
                    record.get("retweeted_mid"), record.get("created_at"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void segmentUidMidBlogFile(String oriFilePath, String resultPath, int index) {
        File file = new File(oriFilePath);
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files == null || files.length == 0) {
                return;
            }
            List<String> filePathList = new ArrayList<>();
            for (File file1 : files) {
                filePathList.add(file1.getPath());
            }
            segmentUidMidBlogFile(filePathList, resultPath, index);
        } else {
            List<String> filePathList = new ArrayList<>();
            filePathList.add(oriFilePath);
            segmentUidMidBlogFile(filePathList, resultPath, index);
        }
    }

    public void segmentUidMidBlogFile2(String oriFilePath, String resultPath, int index) {
        File file = new File(oriFilePath);
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files == null || files.length == 0) {
                return;
            }
            List<String> filePathList = new ArrayList<>();
            for (File file1 : files) {
                filePathList.add(file1.getPath());
            }
            segmentUidMidBlogFile2(filePathList, resultPath, index);
        } else {
            List<String> filePathList = new ArrayList<>();
            filePathList.add(oriFilePath);
            segmentUidMidBlogFile2(filePathList, resultPath, index);
        }
    }

    public void segmentUidMidBlogFile(List<String> filePathList, String resultPath, int index) {
        int count = 0;
        int segmentSize = 6000000;
        long startTime = System.currentTimeMillis();
        log.info("开始分割uid_mid_blog文件");
        File resultFile = new File(resultPath);
        resultFile.mkdirs();
        String fileName = "uid_mid_blog";
        CSVPrinter csvPrinter = null;
        try {
            File newFile = null;
            for (String filePath : filePathList) {
                try (CSVParser csvParser = CsvFileHelper.reader(filePath, HEADER_UID_MID_BLOG, true)) {
                    for (CSVRecord record : csvParser) {
                        if (csvPrinter == null || count >= segmentSize) {
                            if (csvPrinter != null) {
                                log.info("分割写入文件{}完成，耗时{}ms", newFile.getPath(), System.currentTimeMillis() - startTime);
                                csvPrinter.close();
                            }
                            count = 0;
                            newFile = new File(resultPath + File.separator + fileName +
                                    CommonConstants.UNDERLINE + index + FileSuffixConstants.CSV);
                            index++;
                            csvPrinter = CsvFileHelper.writer(newFile, HEADER_UID_MID_BLOG);
                            startTime = System.currentTimeMillis();
                            log.info("开始写入分割文件{}", newFile.getPath());
                        }
                        csvPrinter.printRecord(record);
                        count++;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
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
        log.info("uid_mid_blog文件已分割完成, 共耗时{}ms", System.currentTimeMillis() - startTime);
    }

    public void segmentUidMidBlogFile2(List<String> filePathList, String resultPath, int index) {
        int count = 0;
        int segmentSize = 6000000;
        long startTime = System.currentTimeMillis();
        log.info("开始分割uid_mid_blog文件");
        File resultFile = new File(resultPath);
        resultFile.mkdirs();
        String fileName = "uid_mid_blog";
        CSVPrinter csvPrinter = null;
        try {
            File newFile = null;
            for (String filePath : filePathList) {
                try (CSVParser csvParser = CsvFileHelper.reader(filePath, HEADER_MBLOG3, true)) {
                    for (CSVRecord record : csvParser) {
                        if (csvPrinter == null || count >= segmentSize) {
                            if (csvPrinter != null) {
                                log.info("分割写入文件{}完成，耗时{}ms", newFile.getPath(), System.currentTimeMillis() - startTime);
                                csvPrinter.close();
                            }
                            count = 0;
                            newFile = new File(resultPath + File.separator + fileName +
                                CommonConstants.UNDERLINE + index + FileSuffixConstants.CSV);
                            index++;
                            csvPrinter = CsvFileHelper.writer(newFile, HEADER_UID_MID_BLOG);
                            startTime = System.currentTimeMillis();
                            log.info("开始写入分割文件{}", newFile.getPath());
                        }
                        String text = BlogCleanUtils.cleanBlog(record.get("text"));
//                String retweetedText = BlogCleanUtils.cleanBlog(record.get("retweeted_text"));
                        String retweetedText = "";
                        csvPrinter.printRecord(record.get("uid"), record.get("mid"), text, retweetedText, record.get("created_at"));
                        count++;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
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
        log.info("uid_mid_blog文件已分割完成, 共耗时{}ms", System.currentTimeMillis() - startTime);
    }
}
