package com.cl.data.mapreduce.reducer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.cl.data.mapreduce.bean.TopInput;
import com.cl.data.mapreduce.util.BuildCsvString;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;

/**
 * @author yejianyu
 * @date 2019/9/11
 */
public class UidMidBlogPesgResultReducer extends Reducer<Text, Text, NullWritable, Text> {

    private static final String[] HEADER_PESG = {"uid", "mid", "app", "author", "book", "brand", "business_china",
        "business_group", "business_overseas", "chezaiyongpin", "city", "dianshi", "dongman", "duanpian", "ebook",
        "enterprise", "film", "jilupian", "music", "product", "qichebaoxian", "qichechangshang", "qichechexi",
        "qichechexing", "qichejibie", "qichemeirongchanpin", "qichepeizhi", "qichepinpai", "qicheqita",
        "qichezhengjian", "star_en", "star_zh", "zongyi", "created_at", "weight"};

    private static final Set<String> COMMON_HEADER_SET = new HashSet<String>() {{
        add("uid");
        add("mid");
        add("created_at");
        add("weight");
    }};

//    @Override
//    protected void reduce(Text key, Iterable<TopInput> values, Context context) throws IOException, InterruptedException {
//
//        Set<String> midSet = new HashSet<>();
//        Map<String, Map<String, CountTime>> groupWordCountMap = new HashMap<>();;
//        String uid = key.toString();
//        for (TopInput value : values) {
//            if ("pesg".equals(value.getType().toString())){
//                String s = value.getUid().toString();
//            }
//
//            CSVParser parse = CSVParser.parse(value.toString(), CSVFormat.DEFAULT.withHeader(HEADER_PESG));
//            for (CSVRecord record : parse){
//                String mid = record.get(1);
//                if (!midSet.add(mid)){
//                    continue;
//                }
//
//                String createdAt = record.get("created_at");
//                Timestamp timestamp = Timestamp.valueOf(createdAt);
//
//                for (String group : HEADER_PESG) {
//                    if (COMMON_HEADER_SET.contains(group)) {
//                        continue;
//                    }
//                    String keyword = record.get(group);
//                    Map<String, CountTime> wordCountMap;
//                    if (groupWordCountMap.containsKey(group)) {
//                        wordCountMap = groupWordCountMap.get(group);
//                    } else {
//                        wordCountMap = new HashMap<>();
//                        groupWordCountMap.put(group, wordCountMap);
//                    }
//                    JSONObject jsonObject = JSON.parseObject(keyword);
//                    jsonObject.forEach((word, count) -> {
//                        wordCountMap.merge(word, new CountTime((Integer) count, timestamp), (oldCountTime, newCountTime) -> {
//                            int wordCount = oldCountTime.getCount() + newCountTime.getCount();
//                            oldCountTime.setCount(wordCount);
//                            if (newCountTime.getTimestamp().after(oldCountTime.getTimestamp())) {
//                                oldCountTime.setTimestamp(newCountTime.getTimestamp());
//                            }
//                            return oldCountTime;
//                        });
//                    });
//                }
//            }
//        }
//        Map<String, Integer> typeIndexMap = getTypeIndexMap(HEADER_PESG);
//        String[] resultArray = new String[32];
//        resultArray[0] = String.valueOf(uid);
//        for (Map.Entry<String, Map<String, CountTime>> entry : groupWordCountMap.entrySet()) {
//            String group = entry.getKey();
//            Map<String, CountTime> value = entry.getValue();
//            List<Map.Entry<String, CountTime>> list = new LinkedList<>(value.entrySet());
//            list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));
//
//            Map<String, Integer> result = new LinkedHashMap<>();
//            for (Map.Entry<String, CountTime> wordCountEntry : list) {
//                result.put(wordCountEntry.getKey(), wordCountEntry.getValue().getCount());
//            }
//            resultArray[typeIndexMap.get(group)] = JSON.toJSONString(result, SerializerFeature.UseSingleQuotes);
//        }
//
//        context.write(NullWritable.get(), new Text(BuildCsvString.build(resultArray, ',')));
//    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Set<String> midSet = new HashSet<>();
        Map<String, Map<String, CountTime>> groupWordCountMap = new HashMap<>();;
        String uid = key.toString();
        for (Text value : values) {
            CSVParser parse = CSVParser.parse(value.toString(), CSVFormat.DEFAULT.withHeader(HEADER_PESG));
            for (CSVRecord record : parse){
                String mid = record.get(1);
                if (!midSet.add(mid)){
                    continue;
                }

                String createdAt = record.get("created_at");
                Timestamp timestamp = Timestamp.valueOf(createdAt);

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
        }
        Map<String, Integer> typeIndexMap = getTypeIndexMap(HEADER_PESG);
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
        context.write(NullWritable.get(), new Text(BuildCsvString.build(resultArray, ',')));
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
