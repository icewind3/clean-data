package com.cl.data.hbase;

import com.cl.graph.weibo.core.util.CsvFileHelper;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.List;
//
//@RunWith(SpringRunner.class)
//@SpringBootTest
public class DataHbaseApplicationTests {

    @Test
    public void contextLoads() {
        String filePath = "C:/Users/cl32/Downloads/mblog_from_uid_0_result.csv";
        try (CSVParser csvParser = CsvFileHelper.reader(filePath)) {
            String firstEndOfLine = csvParser.getFirstEndOfLine();
            System.out.println(firstEndOfLine);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void test() {
        String a = "2";
        String b = "25";
        int i = a.compareTo(b);
        System.out.println(i);
    }
}
