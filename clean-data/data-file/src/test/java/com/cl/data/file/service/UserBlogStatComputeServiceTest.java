package com.cl.data.file.service;

import com.cl.graph.weibo.core.util.CsvFileHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/9/12
 */
@Slf4j
public class UserBlogStatComputeServiceTest {

    private static final String[] HEADER_USER_BLOG_STAT = {"uid", "releaseFrequency", "attitudeAvg", "commentAvg", "repostAvg",
        "attitudeMedian", "commentMedian", "repostMedian", "repostRate", "releaseFrequency2", "attitudeAvg2",
        "commentAvg2", "repostAvg2", "attitudeMedian2", "commentMedian2", "repostMedian2", "repostRate2", "attitudeTopAvg",
        "commentTopAvg", "repostTopAvg"};

    @Test
    public void computeRate() {
        int count1 = 0, count2 = 0, count3 = 0, count4 = 0, count5 = 0, count6 = 0, count7 = 0;
        try (CSVParser csvParser = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/user_blog_stat.csv", HEADER_USER_BLOG_STAT)) {
            for (CSVRecord record : csvParser) {
                float releaseFrequency = Float.parseFloat(record.get("releaseFrequency2"));
                if (releaseFrequency == 0) {
                    count1++;
                } else if (releaseFrequency < 10) {
                    count2++;
                } else if (releaseFrequency < 20) {
                    count3++;
                } else if (releaseFrequency < 50) {
                    count4++;
                } else if (releaseFrequency < 100) {
                    count5++;
                } else if (releaseFrequency < 0.9) {
                    count6++;
                } else {
                    count7++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("{},{},{},{},{},{},{}", count1, count2, count3, count4, count5, count6, count7);
    }

    @Test
    public void computeCount() {
        int count1 = 0, count2 = 0, count3 = 0, count4 = 0, count5 = 0, count6 = 0;
        try (CSVParser csvParser = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/user_blog_stat.csv", HEADER_USER_BLOG_STAT)) {
            for (CSVRecord record : csvParser) {
                int num = Integer.parseInt(record.get("repostMedian2"));
                if (num == 0) {
                    count1++;
                } else if (num < 10) {
                    count2++;
                } else if (num < 50) {
                    count3++;
                } else if (num < 100) {
                    count4++;
                } else if (num < 1000) {
                    count5++;
                } else {
                    count6++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("{},{},{},{},{},{}", count1, count2, count3, count4, count5, count6);
    }
}