package com.cl.data.stat.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cl.data.stat.util.FileUtil;
import com.cl.graph.weibo.core.constant.FileSuffixConstants;
import com.cl.graph.weibo.core.util.CsvFileHelper;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * @author yejianyu
 * @date 2019/9/6
 */
@Service
public class KeywordStatisticsService {

    private static final Set<Long> MID_SET = new HashSet<>();
    private static final Map<String, Map<String, Set<Long>>> GROUP_WORD_COUNT_MAP_PEOPLE_STAT =
        new HashMap<>();
    private static final Map<String, Map<String, Map<Long, Integer>>> GROUP_WORD_COUNT_MAP_WORD_FREQUENCY =
        new HashMap<>();


    private static final Set<String> COMMON_HEADER_SET = new HashSet<String>() {{
        add("uid");
        add("mid");
        add("created_at");
        add("weight");
    }};

    public synchronized void addKeywordPeopleStatisticFromFile(String filePath, String[] header) {
        processFile(filePath, oneFilePath -> {
            addKeywordPeopleStatFromOneFile(oneFilePath, header);
        });
    }

    public synchronized void printKeywordPeopleStatResult(String resultPath) {
        printKeywordPeopleStatResult(GROUP_WORD_COUNT_MAP_PEOPLE_STAT, resultPath);
    }

    public void countByPeopleStatisticFromOneFile(String filePath, String resultPath, String[] header) {
        File file = new File(resultPath);
        file.mkdirs();
        Set<Long> midSet = new HashSet<>();
        Map<String, Map<String, Set<Long>>> groupWordCountMap = new HashMap<>();
        computeKeywordPeopleStatFromOneFile(filePath, header, midSet, groupWordCountMap);
        printKeywordPeopleStatResult(groupWordCountMap, resultPath);
    }

    private void processFile(String filePath, Consumer<String> consumer) {
        File file = new File(filePath);
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files == null || files.length == 0) {
                return;
            }
            for (File oneFile : files) {
                consumer.accept(oneFile.getPath());
            }
        } else {
            consumer.accept(filePath);
        }
    }

    private void addKeywordPeopleStatFromOneFile(String filePath, String[] header) {
        computeKeywordPeopleStatFromOneFile(filePath, header, MID_SET, GROUP_WORD_COUNT_MAP_PEOPLE_STAT);
    }

    private void computeKeywordPeopleStatFromOneFile(String filePath, String[] header, Set<Long> midSet,
                                                     Map<String, Map<String, Set<Long>>> groupWordCountMap) {
        try (CSVParser csvParser = CsvFileHelper.reader(filePath, header, true)) {
            for (CSVRecord record : csvParser) {

                Long uid = Long.parseLong(record.get("uid"));

                Long mid = Long.parseLong(record.get("mid"));
                if (midSet.add(mid)) {
                    continue;
                }

                for (String group : header) {
                    if (COMMON_HEADER_SET.contains(group)) {
                        continue;
                    }
                    String keyword = record.get(group);
                    Map<String, Set<Long>> wordCountMap;
                    if (groupWordCountMap.containsKey(group)) {
                        wordCountMap = groupWordCountMap.get(group);
                    } else {
                        wordCountMap = new HashMap<>();
                        groupWordCountMap.put(group, wordCountMap);
                    }
                    JSONObject jsonObject = JSON.parseObject(keyword);
                    jsonObject.keySet().forEach(word -> {
                        Set<Long> uidSet;
                        if (wordCountMap.containsKey(word)) {
                            uidSet = wordCountMap.get(word);
                        } else {
                            uidSet = new HashSet<>();
                            wordCountMap.put(word, uidSet);
                        }
                        uidSet.add(uid);
                    });
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void printKeywordPeopleStatResult(Map<String, Map<String, Set<Long>>> resultMap, String resultPath) {
        File resultFile = new File(resultPath);
        resultFile.mkdirs();
        resultMap.forEach((group, wordCountMap) -> {
            try (CSVPrinter printer = CsvFileHelper.writer(resultPath + File.separator + group
                + FileSuffixConstants.CSV)) {
                for (Map.Entry<String, Set<Long>> entry : wordCountMap.entrySet()) {
                    printer.printRecord(entry.getKey(), entry.getValue().size());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public void countByWordFrequencyFromOneFile(String filePath, String resultPath, String[] header) {
        File file = new File(resultPath);
        file.mkdirs();
        Set<Long> midSet = new HashSet<>();
        Map<String, Map<String, Map<Long, Integer>>> groupWordCountMap = new HashMap<>();
        try (CSVParser csvParser = CsvFileHelper.reader(filePath, header, true)) {
            for (CSVRecord record : csvParser) {

                Long uid = Long.parseLong(record.get("uid"));

                Long mid = Long.parseLong(record.get("mid"));
                if (!midSet.add(mid)) {
                    continue;
                }

                for (String group : header) {
                    if (COMMON_HEADER_SET.contains(group)) {
                        continue;
                    }
                    String keyword = record.get(group);
                    Map<String, Map<Long, Integer>> wordCountMap;
                    if (groupWordCountMap.containsKey(group)) {
                        wordCountMap = groupWordCountMap.get(group);
                    } else {
                        wordCountMap = new HashMap<>();
                        groupWordCountMap.put(group, wordCountMap);
                    }
                    JSONObject jsonObject = JSON.parseObject(keyword);
                    jsonObject.forEach((word, count) -> {
                        Map<Long, Integer> uidCountMap;
                        if (wordCountMap.containsKey(word)) {
                            uidCountMap = wordCountMap.get(word);
                        } else {
                            uidCountMap = new HashMap<>();
                            wordCountMap.put(word, uidCountMap);
                        }
                        try {
                            int wordCount = (int) count;
                            uidCountMap.merge(uid, wordCount, Integer::sum);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        groupWordCountMap.forEach((group, wordCountMap) -> {
            try (CSVPrinter printer = CsvFileHelper.writer(resultPath + File.separator + group
                + FileSuffixConstants.CSV)) {
                for (Map.Entry<String, Map<Long, Integer>> entry : wordCountMap.entrySet()) {
                    String word = entry.getKey();
                    int sum = 0;
                    for (int i : entry.getValue().values()) {
                        sum += Math.sqrt(i);
                    }
                    printer.printRecord(word, sum);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

}
