package com.cl.data.hbase.service;

import com.cl.data.hbase.entity.BlogWeekInfo;
import com.cl.graph.weibo.core.util.CsvFileHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/10/31
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class UserWaterArmyDataStateServiceTest {

    @Resource
    private UserWaterArmyDataStateService userWaterArmyDataStateService;

    @Test
    public void getBlogDailyInfoList() throws IOException {
        String uidPath = "C:\\Users\\cl32\\Documents\\weibo\\weiboBigGraphNew\\uid_core_new/uid_80w.csv";
        String resultPath = "C:\\Users\\cl32\\Desktop\\水军/uid_date_blog_count_80w.csv";
        String[] header = {"uid","date", "original_blog_count", "retweet_blog_count"};
        List<Long> list = new ArrayList<>();
        int count = 0;
        try (CSVParser csvParser = CsvFileHelper.reader(uidPath);
             CSVPrinter csvPrinter = CsvFileHelper.writer(resultPath, header)) {
            for (CSVRecord record : csvParser) {
                String uid = record.get(0);
                list.add(Long.parseLong(uid));
                count++;
                if (list.size() >= 1000) {
                    try {
                        List<BlogWeekInfo> blogWeekInfoList = userWaterArmyDataStateService.getBlogDailyInfoList(list);
                        for (BlogWeekInfo blogWeekInfo : blogWeekInfoList) {
                            long retweetBlogTotal = blogWeekInfo.getRetweetBlogTotal();
                            csvPrinter.printRecord(blogWeekInfo.getUid(), blogWeekInfo.getStartDate(),
                                blogWeekInfo.getBlogTotal() - retweetBlogTotal, retweetBlogTotal);
                        }
                        csvPrinter.flush();
                        list.clear();
                        log.info("complete {}", count);
                    } catch (Exception e) {
                        csvPrinter.flush();
                        log.error("uid={}", list.get(0), e);
                    }

                }
            }
            if (list.size() > 0) {
                List<BlogWeekInfo> blogWeekInfoList = userWaterArmyDataStateService.getBlogDailyInfoList(list);
                for (BlogWeekInfo blogWeekInfo : blogWeekInfoList) {
                    long retweetBlogTotal = blogWeekInfo.getRetweetBlogTotal();
                    csvPrinter.printRecord(blogWeekInfo.getUid(), blogWeekInfo.getStartDate(),
                        blogWeekInfo.getBlogTotal() - retweetBlogTotal, retweetBlogTotal);
                }
            }
        }
        log.info("success {}", count);
    }

    @Test
    public void getOneBlognfoList() throws IOException {
        String uidPath = "C:\\Users\\cl32\\Documents\\weibo\\weiboBigGraphNew\\uid_core_new/uid_80w.csv";
        String resultPath = "C:\\Users\\cl32\\Desktop\\水军/uid_datetime_is_retweeted_80w.csv";
        String[] header = {"uid","datetime", "is_retweeted"};
        List<Long> list = new ArrayList<>();
        int count = 0;
        try (CSVParser csvParser = CsvFileHelper.reader(uidPath);
             CSVPrinter csvPrinter = CsvFileHelper.writer(resultPath, header)) {
            for (CSVRecord record : csvParser) {
                String uid = record.get(0);
                list.add(Long.parseLong(uid));
                count++;
                if (list.size() >= 1000) {
                    try {
                        List<BlogWeekInfo> blogWeekInfoList = userWaterArmyDataStateService.getOneBlognfoList(list);
                        for (BlogWeekInfo blogWeekInfo : blogWeekInfoList) {
                            csvPrinter.printRecord(blogWeekInfo.getUid(), blogWeekInfo.getStartDate(),
                                blogWeekInfo.getRetweetBlogTotal());
                        }
                        csvPrinter.flush();
                        list.clear();
                        log.info("complete {}", count);
                    } catch (Exception e) {
                        csvPrinter.flush();
                        log.error("uid={}", list.get(0), e);
                    }

                }
            }
            if (list.size() > 0) {
                List<BlogWeekInfo> blogWeekInfoList = userWaterArmyDataStateService.getBlogDailyInfoList(list);
                for (BlogWeekInfo blogWeekInfo : blogWeekInfoList) {
                    csvPrinter.printRecord(blogWeekInfo.getUid(), blogWeekInfo.getStartDate(),
                        blogWeekInfo.getRetweetBlogTotal());
                }
            }
        }
        log.info("success {}", count);
    }
}