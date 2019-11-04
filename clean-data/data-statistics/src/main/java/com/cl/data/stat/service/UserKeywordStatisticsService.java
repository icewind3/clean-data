package com.cl.data.stat.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.cl.data.stat.util.FileUtil;
import com.cl.graph.weibo.core.constant.FileSuffixConstants;
import com.cl.graph.weibo.core.util.CsvFileHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.function.Consumer;

/**
 * @author yejianyu
 * @date 2019/9/6
 */
@Slf4j
@Service
public class UserKeywordStatisticsService {

    private static Set<Long> uidSet205w = new HashSet<>();

    private static final String[] HEADER_PESG = {"uid", "mid", "app", "author", "book", "brand", "business_china",
        "business_group", "business_overseas", "chezaiyongpin", "city", "dianshi", "dongman", "duanpian", "ebook",
        "enterprise", "film", "jilupian", "music", "product", "qichebaoxian", "qichechangshang", "qichechexi",
        "qichechexing", "qichejibie", "qichemeirongchanpin", "qichepeizhi", "qichepinpai", "qicheqita",
        "qichezhengjian", "star_en", "star_zh", "zongyi", "created_at", "weight"};

    private static final String[] HEADER_PESG_STAT = {"uid", "app", "author", "book", "brand", "business_china",
        "business_group", "business_overseas", "chezaiyongpin", "city", "dianshi", "dongman", "duanpian", "ebook",
        "enterprise", "film", "jilupian", "music", "product", "qichebaoxian", "qichechangshang", "qichechexi",
        "qichechexing", "qichejibie", "qichemeirongchanpin", "qichepeizhi", "qichepinpai", "qicheqita",
        "qichezhengjian", "star_en", "star_zh", "zongyi"};

    private static final String[] HEADER_KEYWORD_TOP = {"uid", "brand", "product","star_en", "star_zh"};

    static {
        try {
            uidSet205w = FileUtil.readColumn("205w/uid.txt", 0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static final Set<String> COMMON_HEADER_SET = new HashSet<String>() {{
        add("uid");
        add("mid");
        add("created_at");
        add("weight");
    }};

    public void computeUserKeywordStat(String input, String resultPath) {
        Set<Long> midSet = new HashSet<>();
        Map<Long, Map<String, Map<String, CountTime>>> userGroupWordCountMap = new HashMap<>();
        File dirFile = new File(input);
        List<String> filepaths = new ArrayList<>();
        if (dirFile.isDirectory()) {
            File[] files = dirFile.listFiles();
            for(File file : files) {
                filepaths.add(file.getPath());
            }
        } else {
            filepaths.add(input);
        }
        for(String filePath : filepaths) {
            log.info("START read file: {}", filePath);
            try (CSVParser csvParser = CsvFileHelper.reader(filePath, HEADER_PESG)) {
                for (CSVRecord record : csvParser) {
                    Long uid = Long.parseLong(record.get("uid"));
                    Long mid = Long.parseLong(record.get("mid"));
                    if (!midSet.add(mid)) {
                        continue;
                    }

                    String createdAt = record.get("created_at");
                    Timestamp timestamp = Timestamp.valueOf(createdAt);
                    Map<String, Map<String, CountTime>> groupWordCountMap;
                    if (userGroupWordCountMap.containsKey(uid)) {
                        groupWordCountMap = userGroupWordCountMap.get(uid);
                    } else {
                        groupWordCountMap = new HashMap<>();
                        userGroupWordCountMap.put(uid, groupWordCountMap);
                    }
                    for (String group : HEADER_PESG) {
                        if (COMMON_HEADER_SET.contains(group)) {
                            continue;
                        }
                        String keyword = record.get(group);
                        Map<String, CountTime> wordCountMap;
                        if (groupWordCountMap.containsKey(group)) {
                            wordCountMap = groupWordCountMap.get(group);
                        } else {
                            wordCountMap = new HashMap<>();
                            groupWordCountMap.put(group, wordCountMap);
                        }
                        JSONObject jsonObject = JSON.parseObject(keyword);
                        jsonObject.forEach((word, count) -> {
                            wordCountMap.merge(word, new CountTime((Integer) count, timestamp), (oldCountTime, newCountTime) -> {
                                int wordCount = oldCountTime.getCount() + newCountTime.getCount();
                                oldCountTime.setCount(wordCount);
                                if (newCountTime.getTimestamp().after(oldCountTime.getTimestamp())) {
                                    oldCountTime.setTimestamp(newCountTime.getTimestamp());
                                }
                                return oldCountTime;
                            });
                        });
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            log.info("read file success: {}", filePath);
        }
        Map<String, Integer> typeIndexMap = getTypeIndexMap(HEADER_PESG);

        try (CSVPrinter printer = CsvFileHelper.writer(resultPath, HEADER_PESG_STAT)) {
            userGroupWordCountMap.forEach((uid, groupWordCountMap) -> {

                String[] resultArray = new String[32];
                resultArray[0] = String.valueOf(uid);
                for (Map.Entry<String, Map<String, CountTime>> entry : groupWordCountMap.entrySet()) {
                    String group = entry.getKey();
                    Map<String, CountTime> value = entry.getValue();
                    List<Map.Entry<String, CountTime>> list = new LinkedList<>(value.entrySet());
                    list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

                    Map<String, Integer> result = new LinkedHashMap<>();
                    for (Map.Entry<String, CountTime> wordCountEntry : list) {
                        result.put(wordCountEntry.getKey(), wordCountEntry.getValue().getCount());
                    }
                    resultArray[typeIndexMap.get(group)] = JSON.toJSONString(result, SerializerFeature.UseSingleQuotes);
                }
                List<String> resultList = new ArrayList<>(Arrays.asList(resultArray));
                try {
                    printer.printRecord(resultList);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void computeCoreUserKeywordStat(String input, String resultPath) {
        Set<Long> midSet = new HashSet<>();
        Map<Long, Map<String, Map<String, CountTime>>> userGroupWordCountMap = new HashMap<>();
        File dirFile = new File(input);
        List<String> filepaths = new ArrayList<>();
        if (dirFile.isDirectory()) {
            File[] files = dirFile.listFiles();
            for(File file : files) {
                filepaths.add(file.getPath());
            }
        }
        for(String filePath : filepaths) {
            log.info("START read file: {}", filePath);
            try (CSVParser csvParser = CsvFileHelper.reader(filePath, HEADER_PESG, true)) {
                for (CSVRecord record : csvParser) {
                    Long uid = Long.parseLong(record.get("uid"));
                    if (!uidSet205w.contains(uid)) {
                        continue;
                    }
                    Long mid = Long.parseLong(record.get("mid"));
                    if (!midSet.add(mid)) {
                        continue;
                    }

                    String createdAt = record.get("created_at");
                    Timestamp timestamp = Timestamp.valueOf(createdAt);
                    Map<String, Map<String, CountTime>> groupWordCountMap;
                    if (userGroupWordCountMap.containsKey(uid)) {
                        groupWordCountMap = userGroupWordCountMap.get(uid);
                    } else {
                        groupWordCountMap = new HashMap<>();
                        userGroupWordCountMap.put(uid, groupWordCountMap);
                    }
                    for (String group : HEADER_PESG) {
                        if (COMMON_HEADER_SET.contains(group)) {
                            continue;
                        }
                        String keyword = record.get(group);
                        Map<String, CountTime> wordCountMap;
                        if (groupWordCountMap.containsKey(group)) {
                            wordCountMap = groupWordCountMap.get(group);
                        } else {
                            wordCountMap = new HashMap<>();
                            groupWordCountMap.put(group, wordCountMap);
                        }
                        JSONObject jsonObject = JSON.parseObject(keyword);
                        jsonObject.forEach((word, count) -> {
                            wordCountMap.merge(word, new CountTime((Integer) count, timestamp), (oldCountTime, newCountTime) -> {
                                int wordCount = oldCountTime.getCount() + newCountTime.getCount();
                                oldCountTime.setCount(wordCount);
                                if (newCountTime.getTimestamp().after(oldCountTime.getTimestamp())) {
                                    oldCountTime.setTimestamp(newCountTime.getTimestamp());
                                }
                                return oldCountTime;
                            });
                        });
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
           log.info("read file success: {}", filePath);
        }
        Map<String, Integer> typeIndexMap = getTypeIndexMap(HEADER_PESG);

        try (CSVPrinter printer = CsvFileHelper.writer(resultPath, HEADER_PESG_STAT)) {
            userGroupWordCountMap.forEach((uid, groupWordCountMap) -> {

                String[] resultArray = new String[32];
                resultArray[0] = String.valueOf(uid);
                for (Map.Entry<String, Map<String, CountTime>> entry : groupWordCountMap.entrySet()) {
                    String group = entry.getKey();
                    Map<String, CountTime> value = entry.getValue();
                    List<Map.Entry<String, CountTime>> list = new LinkedList<>(value.entrySet());
                    list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

                    Map<String, Integer> result = new LinkedHashMap<>();
                    for (Map.Entry<String, CountTime> wordCountEntry : list) {
                        result.put(wordCountEntry.getKey(), wordCountEntry.getValue().getCount());
                    }
                    resultArray[typeIndexMap.get(group)] = JSON.toJSONString(result, SerializerFeature.UseSingleQuotes);
                }
                List<String> resultList = new ArrayList<>(Arrays.asList(resultArray));
                try {
                    printer.printRecord(resultList);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void getUserKeywordStatTop(String filePath, String resultPath) {
        int topSize = 5;
        try (CSVPrinter printer = CsvFileHelper.writer(resultPath, HEADER_KEYWORD_TOP);
             CSVParser csvParser = CsvFileHelper.reader(filePath, HEADER_PESG_STAT, true)) {
            for (CSVRecord record : csvParser) {
                String brand = record.get("brand");
                String product = record.get("product");
                String starEn = record.get("star_en");
                String statZh = record.get("star_zh");
                String uid = record.get("uid");
                printer.printRecord(uid, getTopKeywordJsonStr(brand, topSize), getTopKeywordJsonStr(product, topSize),
                    getTopKeywordJsonStr(starEn, topSize), getTopKeywordJsonStr(statZh, topSize));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getTopKeywordJsonStr(String keyword, int size) {
        Map<String, Integer> topMap = new LinkedHashMap<>();
        JSONObject jsonObject = JSON.parseObject(keyword, Feature.OrderedField);
        int num = 0;
        for (String word : jsonObject.keySet()){
            if (num >= size){
                break;
            }
            topMap.put(word, (Integer) jsonObject.get(word));
            num++;
        }
        return JSON.toJSONString(topMap, SerializerFeature.UseSingleQuotes);
    }

    private Map<String, Integer> getTypeIndexMap(String[] header) {
        int index = 1;
        Map<String, Integer> map = new HashMap<>();
        for (String group : header) {
            if (COMMON_HEADER_SET.contains(group)) {
                continue;
            }
            map.put(group, index);
            index++;
        }
        return map;
    }

    class CountTime implements Comparable<CountTime> {
        int count;
        Timestamp timestamp;

        CountTime(int count, Timestamp timestamp) {
            this.count = count;
            this.timestamp = timestamp;
        }

        @Override
        public int compareTo(CountTime o) {
            if (this.count > o.count) {
                return 1;
            }
            if (this.count < o.count) {
                return -1;
            }
            if (this.timestamp.after(o.timestamp)) {
                return 1;
            }
            if (this.timestamp.before(o.timestamp)) {
                return -1;
            }
            return 0;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public Timestamp getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Timestamp timestamp) {
            this.timestamp = timestamp;
        }
    }

}
