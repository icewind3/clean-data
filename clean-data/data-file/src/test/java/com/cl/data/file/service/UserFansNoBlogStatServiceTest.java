package com.cl.data.file.service;

import com.cl.graph.weibo.core.util.CsvFileHelper;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/10/15
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class UserFansNoBlogStatServiceTest {

    @Resource
    private UserFansNoBlogStatService userFansNoBlogStatService;

    @Test
    public void mergeUid() throws IOException {
        String filePath  = "C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\uid_check/uid_exist";
        String resultPath  = "C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\uid_check/uid_exist.csv";
//        String filePath  = "C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\uid_check/uid_not_exist";
//        String resultPath  = "C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\uid_check/uid_not_exist.csv";
        userFansNoBlogStatService.mergeUid(filePath, resultPath);
    }

    @Test
    public void a() {
        String fansRange = "15w_30w";
        String filePath = "C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\无博文粉丝统计\\" + fansRange
            + "/UserFansStat.csv";
        String resultPath = "uid_fans_LT1000_" + fansRange + ".csv";
        String resultPath2 = "uid_zombie_fan_gte50_" + fansRange + ".csv";
        try (CSVParser csvParser = CsvFileHelper.reader(filePath);
             CSVPrinter writer1 = CsvFileHelper.writer("C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\无博文粉丝统计\\粉丝过少/" + resultPath);
             CSVPrinter writer2 = CsvFileHelper.writer("C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\无博文粉丝统计\\僵尸粉过多/" + resultPath2)) {
            for (CSVRecord record : csvParser) {
                String uid = record.get(0);
                long fansCount = Long.parseLong(record.get(1));
                if (fansCount < 1000) {
                    writer1.printRecord(uid);
                } else {
                    float ratio = Float.parseFloat(record.get(3));
                    if (ratio >= 0.5f) {
                        writer2.printRecord(uid);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test() throws Exception {
        String filePath = "C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\kol_fans\\15w_30w_6";
//        String uidFilePath = "C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\无博文粉丝统计\\僵尸粉大于百分之50/uid_zombie_fan_gte50_15w_30w.csv";
        String uidFilePath = "C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\无博文粉丝统计\\粉丝少于1000/uid_fans_LT1000_15w_30w.csv";
        Set<String> set = new HashSet<>();
        try (CSVParser csvParser = CsvFileHelper.reader(uidFilePath)) {
            for (CSVRecord record : csvParser) {
                set.add(record.get(0));
            }
        }
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
        for (File file : list) {
            String uid =  file.getName();
            if (set.contains(uid)) {
                file.delete();
            }
        }
    }

    @Test
    public void test2() throws Exception {
        String uidFilePath = "C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\无博文粉丝统计/NoBlogFansUid.csv";
        Set<Long> set = new HashSet<>();
        try (CSVParser csvParser = CsvFileHelper.reader(uidFilePath)) {
            for (CSVRecord record : csvParser) {
                set.add(Long.parseLong(record.get(0)));
            }
        }

        String kolUid = "1036663592";
        String filePath = "C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\kol_fans\\500w/" + kolUid;
        String resultPath = "C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\kol_has_blog/" + kolUid;
        try (CSVParser csvParser = CsvFileHelper.reader(filePath);
             CSVPrinter writer1 = CsvFileHelper.writer(resultPath)) {
            for (CSVRecord record : csvParser) {
                String uid = record.get(0);
                if (!set.contains(Long.parseLong(uid))) {
                    writer1.printRecord(uid);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}