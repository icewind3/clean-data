package com.cl.data.user;

import com.cl.graph.weibo.core.util.CsvFileHelper;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

//@RunWith(SpringRunner.class)
//@SpringBootTest
public class UserFilterApplicationTests {

    @Test
    public void contextLoads() {
    }


    @Test
    public void filter() {
        Set<String> uidSet = new HashSet<>();

        try (CSVParser csvParser = CsvFileHelper.reader("C:\\Users\\cl32\\Desktop\\stat\\user_filter_new/uid_core_delete3.csv")){
            for (CSVRecord record : csvParser) {
                uidSet.add(record.get(0));
            }
        }catch (IOException e) {
            e.printStackTrace();
        }

        try (CSVParser csvParser = CsvFileHelper.reader("C:\\Users\\cl32\\Desktop\\stat\\user_filter_new/user_blog_stat_220w_suspected_match_2.csv");
             CSVPrinter csvPrinter = CsvFileHelper.writer("C:\\Users\\cl32\\Desktop\\stat\\user_filter_new\\user_suspected_new/uid_suspected.csv")){
            for (CSVRecord record : csvParser) {
                String uid = record.get(0);
                if (!uidSet.contains(uid)){
                    csvPrinter.printRecord(uid);
                }
            }
        }catch (IOException e) {
            e.printStackTrace();
        }

    }

}
