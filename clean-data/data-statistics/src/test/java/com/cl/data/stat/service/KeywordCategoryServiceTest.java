package com.cl.data.stat.service;

import com.cl.graph.weibo.core.util.CsvFileHelper;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/10/25
 */
public class KeywordCategoryServiceTest {

    @Test
    public void computePercent() throws IOException {
        int num = 71588677;
        String type = "zongyi";
        String file = "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\人数统计_20191017\\category_人数_20191104/" + type
            + ".csv";
        Map<String, Long> map = new HashMap<>();
        try (CSVParser csvParser = CsvFileHelper.reader(file)) {
            for (CSVRecord record : csvParser) {
                map.put(record.get(0), Long.parseLong(record.get(1)));
            }
        }
        List<Map.Entry<String, Long>> list = new ArrayList<>(map.entrySet());
        list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));
        try (CSVPrinter csvPrinter = CsvFileHelper.writer(
            "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\人数统计_20191017\\category_人数_20191104/提到的人数/"
                + type + ".csv");
             CSVPrinter csvPrinter2 = CsvFileHelper.writer(
                 "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\人数统计_20191017\\category_人数_20191104/提到的人数除以总人数/"
                     + type + "_percent.csv");) {
            for (Map.Entry<String, Long> entry : list) {
                csvPrinter.printRecord(entry.getKey(), entry.getValue());
                csvPrinter2.printRecord(entry.getKey(), (float) entry.getValue() / num);
            }
        }

    }

}