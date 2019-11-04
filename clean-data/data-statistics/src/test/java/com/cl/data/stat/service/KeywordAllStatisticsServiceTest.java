package com.cl.data.stat.service;

import com.cl.graph.weibo.core.util.CsvFileHelper;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/9/29
 */
public class KeywordAllStatisticsServiceTest {

    @Test
    public void computeCategory() throws IOException {

        int userNum = 15299216;
        long wordCount = 0;
        String type = "zongyi";
        String typeFile = type + ".csv";
        Map<String, String> categoryMap = new HashMap<>();
        try (CSVParser csvParser = CsvFileHelper.reader("C:\\Users\\cl32\\Documents\\weibo\\词库分类"
            + File.separator + typeFile)) {
            for (CSVRecord record : csvParser) {
                categoryMap.put(record.get(0), record.get(1));
            }
        }
        Map<String, Long> categoryCount = new HashMap<>();
        try (CSVParser csvParser = CsvFileHelper.reader(
            "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\画像统计\\微博全量博文画像\\全部词频/" + File.separator + typeFile)) {
            for (CSVRecord record : csvParser) {
                String word = record.get(0);
                String count = record.get(1);
                String category = categoryMap.getOrDefault(word, "其他");
                categoryCount.merge(category, Long.parseLong(count), Long::sum);
                wordCount += Long.parseLong(count);
            }
        }
        List<Map.Entry<String, Long>> list = new ArrayList<>(categoryCount.entrySet());

        list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

        try (CSVPrinter csvPrinter = CsvFileHelper.writer(
            "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\画像统计\\category3" + File.separator + typeFile);
             CSVPrinter csvPrinter2 = CsvFileHelper.writer(
                 "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\画像统计\\category3" + File.separator + type + "_指数.csv");
             CSVPrinter csvPrinter3 = CsvFileHelper.writer(
                 "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\画像统计\\category3" + File.separator + type + "_相对热度.csv")) {
            for (Map.Entry<String, Long> entry : list) {
                csvPrinter.printRecord(entry.getKey(), entry.getValue());
                csvPrinter2.printRecord(entry.getKey(), (float) entry.getValue() / userNum);
                csvPrinter3.printRecord(entry.getKey(), (float) entry.getValue() / wordCount);
            }
        }
    }

    @Test
    public void computeCategoryNew() throws IOException {

        int userNum = 71588677;
//        int userNum = 15299216;
        long wordCount = 0;
        String type = "music";
        String typeFile = type + ".csv";
        Map<String, Set<String>> categoryMap = new HashMap<>();
        try (CSVParser csvParser = CsvFileHelper.reader("C:\\Users\\cl32\\Documents\\weibo\\词库分类"
            + File.separator + typeFile)) {
            for (CSVRecord record : csvParser) {
                String word  = record.get(0);
                String category  = record.get(1).trim();
                if (categoryMap.containsKey(word)){
                    Set<String> set = categoryMap.get(word);
                    set.add(category);
                } else {
                    Set<String> set = new HashSet<>();
                    set.add(category);
                    categoryMap.put(word, set);
                }
            }
        }
        Map<String, Long> categoryCount = new HashMap<>();
        try (CSVParser csvParser = CsvFileHelper.reader(
            "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\人数统计_20191017\\词_提到的人数(删除无效词后)" + File.separator + typeFile)) {
            for (CSVRecord record : csvParser) {
                String word = record.get(0);
                long count = Long.parseLong(record.get(1));
                Set<String> categorySet = categoryMap.get(word);
                if (categorySet == null || categorySet.size() == 0) {
                    categoryCount.merge("其他", count, Long::sum);
                    wordCount += count;
                } else {
                    for (String category : categorySet){
                        categoryCount.merge(category, count, Long::sum);
                        wordCount += count;
                    }
                }
            }
        }
        List<Map.Entry<String, Long>> list = new ArrayList<>(categoryCount.entrySet());

        list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

        try (CSVPrinter csvPrinter = CsvFileHelper.writer(
            "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\人数统计_20191017\\category_人数" + File.separator + typeFile);
             CSVPrinter csvPrinter2 = CsvFileHelper.writer(
                 "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\人数统计_20191017\\category_人数" + File.separator + type + "_percent.csv")) {
            for (Map.Entry<String, Long> entry : list) {
                csvPrinter.printRecord(entry.getKey(), entry.getValue());
                csvPrinter2.printRecord(entry.getKey(), (float) entry.getValue() / userNum);
            }
        }

//        try (CSVPrinter csvPrinter = CsvFileHelper.writer(
//            "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\画像统计\\category3" + File.separator + typeFile);
//             CSVPrinter csvPrinter2 = CsvFileHelper.writer(
//                 "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\画像统计\\category3" + File.separator + type + "_指数.csv");
//             CSVPrinter csvPrinter3 = CsvFileHelper.writer(
//                 "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\画像统计\\category3" + File.separator + type + "_相对热度.csv")) {
//            for (Map.Entry<String, Long> entry : list) {
//                csvPrinter.printRecord(entry.getKey(), entry.getValue());
//                csvPrinter2.printRecord(entry.getKey(), (float) entry.getValue() / userNum);
//                csvPrinter3.printRecord(entry.getKey(), (float) entry.getValue() / wordCount);
//            }
//        }
    }

    @Test
    public void computeCategoryNew2() throws IOException {

//        int userNum = 15299216;
        int userNum = 71588677;
        long wordCount = 0;
        String type = "star_gender";
        String typeFile = type + ".csv";
        Map<String, Set<String>> categoryMap = new HashMap<>();
        try (CSVParser csvParser = CsvFileHelper.reader("C:\\Users\\cl32\\Documents\\weibo\\词库分类"
            + File.separator + typeFile)) {
            for (CSVRecord record : csvParser) {
                String word  = record.get(0);
                String category  = record.get(1).trim();
                if (categoryMap.containsKey(word)){
                    Set<String> set = categoryMap.get(word);
                    set.add(category);
                } else {
                    Set<String> set = new HashSet<>();
                    set.add(category);
                    categoryMap.put(word, set);
                }
            }
        }
        Map<String, Long> categoryCount = new HashMap<>();
        try (CSVParser csvParser = CsvFileHelper.reader(
            "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\人数统计_20191017\\词_提到的人数(删除无效词后)" + File.separator + "star_zh.csv")) {
            for (CSVRecord record : csvParser) {
                String word = record.get(0);
                long count = Long.parseLong(record.get(1));
                Set<String> categorySet = categoryMap.get(word);
                if (categorySet == null || categorySet.size() == 0) {
                    categoryCount.merge("其他", count, Long::sum);
                    wordCount += count;
                } else {
                    for (String category : categorySet){
                        categoryCount.merge(category, count, Long::sum);
                        wordCount += count;
                    }
                }
            }
        }
        try (CSVParser csvParser = CsvFileHelper.reader(
            "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\人数统计_20191017\\词_提到的人数(删除无效词后)" + File.separator + "star_en.csv")) {
            for (CSVRecord record : csvParser) {
                String word = record.get(0);
                long count = Long.parseLong(record.get(1));
                Set<String> categorySet = categoryMap.get(word);
                if (categorySet == null || categorySet.size() == 0) {
                    categoryCount.merge("其他", count, Long::sum);
                    wordCount += count;
                } else {
                    for (String category : categorySet){
                        categoryCount.merge(category, count, Long::sum);
                        wordCount += count;
                    }
                }
            }
        }
        List<Map.Entry<String, Long>> list = new ArrayList<>(categoryCount.entrySet());

        list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

        try (CSVPrinter csvPrinter = CsvFileHelper.writer(
            "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\人数统计_20191017\\category_人数" + File.separator + typeFile);
             CSVPrinter csvPrinter2 = CsvFileHelper.writer(
                 "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\人数统计_20191017\\category_人数" + File.separator + type + "_percent.csv")) {
            for (Map.Entry<String, Long> entry : list) {
                csvPrinter.printRecord(entry.getKey(), entry.getValue());
                csvPrinter2.printRecord(entry.getKey(), (float) entry.getValue() / userNum);
            }
        }

//
//        try (CSVPrinter csvPrinter = CsvFileHelper.writer(
//            "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\画像统计\\category3" + File.separator + typeFile);
//             CSVPrinter csvPrinter2 = CsvFileHelper.writer(
//                 "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\画像统计\\category3" + File.separator + type + "_指数.csv");
//             CSVPrinter csvPrinter3 = CsvFileHelper.writer(
//                 "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\画像统计\\category3" + File.separator + type + "_相对热度.csv")) {
//            for (Map.Entry<String, Long> entry : list) {
//                csvPrinter.printRecord(entry.getKey(), entry.getValue());
//                csvPrinter2.printRecord(entry.getKey(), (float) entry.getValue() / userNum);
//                csvPrinter3.printRecord(entry.getKey(), (float) entry.getValue() / wordCount);
//            }
//        }
    }

    @Test
    public void computeCategory2() throws IOException {

        int userNum = 15299216;
        long wordCount = 0;
        String type = "star_filter";
        String typeFile = type + ".csv";
        Map<String, String> categoryMap = new HashMap<>();
        try (CSVParser csvParser = CsvFileHelper.reader("C:\\Users\\cl32\\Documents\\weibo\\词库分类"
            + File.separator + typeFile)) {
            for (CSVRecord record : csvParser) {
                categoryMap.put(record.get(0), record.get(1).trim());
            }
        }
        Map<String, Long> categoryCount = new HashMap<>();
        try (CSVParser csvParser = CsvFileHelper.reader(
            "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\画像统计\\微博全量博文画像\\全部词频/" + File.separator + "star_zh.csv")) {
            for (CSVRecord record : csvParser) {
                String word = record.get(0);
                String count = record.get(1);
                String category = categoryMap.getOrDefault(word, "其他");
                categoryCount.merge(category, Long.parseLong(count), Long::sum);
                wordCount += Long.parseLong(count);
            }
        }
        try (CSVParser csvParser = CsvFileHelper.reader(
            "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\画像统计\\微博全量博文画像\\全部词频/" + File.separator + "star_en.csv")) {
            for (CSVRecord record : csvParser) {
                String word = record.get(0);
                String count = record.get(1);
                String category = categoryMap.getOrDefault(word, "其他");
                categoryCount.merge(category, Long.parseLong(count), Long::sum);
                wordCount += Long.parseLong(count);
            }
        }
        List<Map.Entry<String, Long>> list = new ArrayList<>(categoryCount.entrySet());

        list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

        try (CSVPrinter csvPrinter = CsvFileHelper.writer(
            "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\画像统计\\category3" + File.separator + typeFile);
             CSVPrinter csvPrinter2 = CsvFileHelper.writer(
                 "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\画像统计\\category3" + File.separator + type + "_指数.csv");
             CSVPrinter csvPrinter3 = CsvFileHelper.writer(
                 "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\画像统计\\category3" + File.separator + type + "_相对热度.csv")) {
            for (Map.Entry<String, Long> entry : list) {
                csvPrinter.printRecord(entry.getKey(), entry.getValue());
                csvPrinter2.printRecord(entry.getKey(), (float) entry.getValue() / userNum);
                csvPrinter3.printRecord(entry.getKey(), (float) entry.getValue() / wordCount);
            }
        }
    }

    @Test
    public void addCategory() throws IOException {

        String type = "zone";
        String typeFile = type + ".csv";
        Map<String, String> categoryMap = new HashMap<>();
        Set<String> set = new HashSet<>();
        try (CSVParser csvParser = CsvFileHelper.reader("C:\\Users\\cl32\\Documents\\weibo\\词库分类"
            + File.separator + typeFile)) {
            for (CSVRecord record : csvParser) {
                String word = record.get(0);
                String category = record.get(1).trim();
                if (set.add(word + "_" + category)){
                    categoryMap.merge(record.get(0), record.get(1).trim(), (s, s2) -> s + "," + s2);
                }
            }
        }
        try (CSVParser csvParser = CsvFileHelper.reader("C:\\Users\\cl32\\Documents\\weibo\\统计结果\\人数统计_20191017\\词_提到的人数"
            + File.separator + typeFile);
             CSVPrinter csvPrinter = CsvFileHelper.writer(
                 "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\人数统计_20191017\\词_category_人数" + File.separator + typeFile)) {
            for (CSVRecord record : csvParser) {
                String word = record.get(0);
                String count = record.get(1);
                String category = categoryMap.getOrDefault(word, "其他");
                csvPrinter.printRecord(word, category, count);
            }
        }
    }

    @Test
    public void addCategory2() throws IOException {

        String type = "star_nation";
        String typeFile = type + ".csv";
        Map<String, String> categoryMap = new HashMap<>();
        Set<String> set = new HashSet<>();
        try (CSVParser csvParser = CsvFileHelper.reader("C:\\Users\\cl32\\Documents\\weibo\\词库分类"
            + File.separator + typeFile)) {
            for (CSVRecord record : csvParser) {
                String word = record.get(0);
                String category = record.get(1).trim();
                if (set.add(word + "_" + category)){
                    categoryMap.merge(record.get(0), record.get(1).trim(), (s, s2) -> s + "," + s2);
                }
            }
        }
        String fileName = "star_zh.csv";
        String resultFileName = "star_zh_nation.csv";
        try (CSVParser csvParser = CsvFileHelper.reader("C:\\Users\\cl32\\Documents\\weibo\\统计结果\\人数统计_20191017\\词_提到的人数"
            + File.separator + fileName);
             CSVPrinter csvPrinter = CsvFileHelper.writer(
                 "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\人数统计_20191017\\词_category_人数" + File.separator + resultFileName)) {
            for (CSVRecord record : csvParser) {
                String word = record.get(0);
                String count = record.get(1);
                String category = categoryMap.getOrDefault(word, "其他");
                csvPrinter.printRecord(word, category, count);
            }
        }
    }

    /**
     * 删除无效词
     */
    @Test
    public void removeWord() throws IOException {
        String typeFileName = "star_en.csv";
        String deleteWordPath = "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\词库删除/star_del.txt";
        String filePath = "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\人数统计_20191017\\词_提到的人数/"
            + typeFileName;
        String resultPath =
            "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\人数统计_20191017\\词_提到的人数(删除无效词后)/"
                + typeFileName;
        Set<String> deleteSet = CsvFileHelper.getStringHashSet(deleteWordPath);
        try (CSVParser csvParser = CsvFileHelper.reader(filePath);
        CSVPrinter csvPrinter = CsvFileHelper.writer(resultPath)){
            for (CSVRecord record : csvParser) {
                if (!deleteSet.contains(record.get(0))){
                    csvPrinter.printRecord(record);
                }
            }
        }
    }

}