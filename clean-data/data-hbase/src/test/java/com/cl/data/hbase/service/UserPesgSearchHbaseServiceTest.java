package com.cl.data.hbase.service;

import com.cl.graph.weibo.core.util.CsvFileHelper;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.assertj.core.util.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/10/18
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class UserPesgSearchHbaseServiceTest {

    @Resource
    private UserPesgSearchHbaseService userPesgSearchHbaseService;

    @Test
    public void getUserBlogPesgResult() throws IOException {

        String type = "comment";
//        String type = "attitude";
//        String type = "repost";
        String filePath = "C:\\Users\\cl32\\Desktop\\AllBirds\\median30Blog/uid_mid_median30_" + type + ".csv";
        Map<Long, List<Long>> map = new HashMap<>();
        try (CSVParser csvParser = CsvFileHelper.reader(filePath)) {
            for (CSVRecord record : csvParser) {
                Long uid = Long.parseLong(record.get(0));
                Long mid = Long.parseLong(record.get(1));
                List<Long> midList;
                if (map.containsKey(uid)) {
                    midList = map.get(uid);
                } else {
                    midList = new ArrayList<>();
                    map.put(uid, midList);
                }
                midList.add(mid);
            }
        }

        List<Long> uidList = new ArrayList<>();
        String uidFilePath = "C:\\Users\\cl32\\Desktop\\AllBirds/uid_kol_287.txt";
        String resultPath = "C:\\Users\\cl32\\Desktop\\AllBirds\\word_count/uid_pesg_stat_median30_" + type + ".csv";
        try (CSVParser parser = CsvFileHelper.reader(uidFilePath)) {
            for (CSVRecord record : parser) {
                uidList.add(Long.parseLong(record.get(0)));
            }
        }
        try (CSVPrinter csvPrinter = CsvFileHelper.writer(resultPath)) {
            uidList.forEach(uid -> {
                String[] userBlogPesgResult = userPesgSearchHbaseService.getUserBlogPesgResult(uid, map.get(uid));
                try {
                    csvPrinter.printRecord(Lists.newArrayList(userBlogPesgResult));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

    }

    @Test
    public void test() throws IOException {
        Set<String> uidSet = new HashSet<>();
        String uidFilePath = "C:\\Users\\cl32\\Desktop\\AllBirds/uid.csv";
        try (CSVParser parser = CsvFileHelper.reader(uidFilePath)) {
            for (CSVRecord record : parser) {
                uidSet.add(record.get(0));
            }
        }

        String uidFilePath2 = "C:\\Users\\cl32\\Desktop\\AllBirds/uid_kol_287.txt";
        String resultPath = "C:\\Users\\cl32\\Desktop\\AllBirds/uid_kol_not_in_80w.csv";
        try (CSVParser parser = CsvFileHelper.reader(uidFilePath2);
             CSVPrinter printer = CsvFileHelper.writer(resultPath)) {
            for (CSVRecord record : parser) {
                String uid  = record.get(0);
                if (!uidSet.contains(uid)) {
                    printer.printRecord(uid);
                }
            }
        }

    }
}