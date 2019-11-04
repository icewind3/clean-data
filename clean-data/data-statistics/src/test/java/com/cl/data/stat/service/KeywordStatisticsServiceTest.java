package com.cl.data.stat.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cl.graph.weibo.core.constant.FileSuffixConstants;
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

/**
 * @author yejianyu
 * @date 2019/9/6
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class KeywordStatisticsServiceTest {

    @Resource
    private KeywordStatisticsService keywordStatisticsService;

    @Test
    public void countByPeopleStatistic() {
        String filePath = "C:/Users/cl32/Downloads/keyword/mblog_from_uid_45_result.csv";
        String resultPath = "C:/Users/cl32/Downloads/keyword/result2";
        String[] header = {"uid", "mid", "ebook", "enterprise", "music", "film", "city", "product", "brand", "star",
            "app", "created_at", "weight"};
        keywordStatisticsService.countByPeopleStatisticFromOneFile(filePath, resultPath, header);
    }

    @Test
    public void countByWordFrequency() {
        String filePath = "C:/Users/cl32/Downloads/keyword/mblog_from_uid_45_result.csv";
        String resultPath = "C:/Users/cl32/Downloads/keyword/result3";
        String[] header = {"uid", "mid", "ebook", "enterprise", "music", "film", "city", "product", "brand", "star",
            "app", "created_at", "weight"};
        keywordStatisticsService.countByWordFrequencyFromOneFile(filePath, resultPath, header);
    }

    @Test
    public void countByWordFrequency2() {
        String filePath = "C:\\Users\\cl32\\Desktop\\wordCount/new/uidWordCount2.csv";
        String resultPath = "C:\\Users\\cl32\\Desktop\\wordCount/new/词频统计";
        String[] header = {"uid", "product", "brand", "starEn", "starZh", "app", "city", "music"};
        File file = new File(resultPath);
        file.mkdirs();
        int uidCount = 0;
        Map<String, Map<String, Integer>> groupWordCountMap = new HashMap<>();
        try (CSVParser csvParser = CsvFileHelper.reader(filePath, header)) {
            for (CSVRecord record : csvParser) {
                uidCount++;
                for (String group : header) {
                    if ("uid".equals(group)) {
                        continue;
                    }
                    String keyword = record.get(group);
                    Map<String, Integer> wordCountMap;
                    if (groupWordCountMap.containsKey(group)) {
                        wordCountMap = groupWordCountMap.get(group);
                    } else {
                        wordCountMap = new HashMap<>();
                        groupWordCountMap.put(group, wordCountMap);
                    }
                    JSONObject jsonObject = JSON.parseObject(keyword);
                    jsonObject.forEach((word, count) -> {
//                        wordCountMap.merge(word, (Integer) count, (integer, integer2) ->
//                            (int) (integer + Math.sqrt(integer2)));
                        wordCountMap.merge(word, (Integer) count, Integer::sum);
//                        wordCountMap.merge(word, 1, Integer::sum);
                    });
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        final int num = uidCount;
        groupWordCountMap.forEach((group, wordCountMap) -> {
            try (CSVPrinter printer = CsvFileHelper.writer(resultPath + File.separator + group
                + FileSuffixConstants.CSV);
                 CSVPrinter printer2 = CsvFileHelper.writer(resultPath + File.separator + group + "_top100_"
                     + FileSuffixConstants.CSV);
                 CSVPrinter printer3 = CsvFileHelper.writer(resultPath + File.separator + group + "_avg_"
                     + FileSuffixConstants.CSV);
                 CSVPrinter printer4 = CsvFileHelper.writer(resultPath + File.separator + group + "_avgPer100_"
                     + FileSuffixConstants.CSV)) {
                List<Map.Entry<String, Integer>> list = new ArrayList<>(wordCountMap.entrySet());

                list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));
                int count = 0;
                for (Map.Entry<String, Integer> entry : list) {
                    count++;
                    printer.printRecord(entry.getKey(), entry.getValue());
                    printer3.printRecord(entry.getKey(), (float) entry.getValue() / num);
                    printer4.printRecord(entry.getKey(), (float) entry.getValue() * 100 / num);
                    if (count <= 100){
                        printer2.printRecord(entry.getKey(), entry.getValue());
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        System.out.println("总人数为: " + uidCount);
    }


    @Test
    public void countCategoryByPeople() {
        String filePath = "C:\\Users\\cl32\\Desktop\\wordCount/new/uidCategoryFreq2.csv";
        String resultPath = "C:\\Users\\cl32\\Desktop\\wordCount/new/类别统计/词频开根号统计";
        String[] header = {"uid", "product", "brand", "starEn", "starZh", "app", "city", "music"};
        File file = new File(resultPath);
        file.mkdirs();
        int uidCount = 0;
        Map<String, Map<String, Integer>> groupWordCountMap = new HashMap<>();
        try (CSVParser csvParser = CsvFileHelper.reader(filePath, header)) {
            for (CSVRecord record : csvParser) {
                uidCount++;
                for (String group : header) {
                    if ("uid".equals(group)) {
                        continue;
                    }
                    String keyword = record.get(group);
                    Map<String, Integer> wordCountMap;
                    if (groupWordCountMap.containsKey(group)) {
                        wordCountMap = groupWordCountMap.get(group);
                    } else {
                        wordCountMap = new HashMap<>();
                        groupWordCountMap.put(group, wordCountMap);
                    }
                    JSONObject jsonObject = JSON.parseObject(keyword);
                    jsonObject.forEach((word, count) -> {
                        wordCountMap.merge(word, (Integer) count, (integer, integer2) ->
                            (int) (integer + Math.sqrt(integer2)));
                        wordCountMap.merge(word, (Integer) count, Integer::sum);
//                        wordCountMap.merge(word, 1, Integer::sum);
                    });
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        final int num = uidCount;
        groupWordCountMap.forEach((group, wordCountMap) -> {
            try (CSVPrinter printer = CsvFileHelper.writer(resultPath + File.separator + group
                + FileSuffixConstants.CSV);
                 CSVPrinter printer2 = CsvFileHelper.writer(resultPath + File.separator + group + "_avg_"
                     + FileSuffixConstants.CSV)) {
                List<Map.Entry<String, Integer>> list = new ArrayList<>(wordCountMap.entrySet());

                list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));
                for (Map.Entry<String, Integer> entry : list) {
                    printer.printRecord(entry.getKey(), entry.getValue());
                    printer2.printRecord(entry.getKey(), (float) entry.getValue() / num);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        System.out.println("总人数为: " + uidCount);
    }



}