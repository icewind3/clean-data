package com.cl.data.mapreduce.reducer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.cl.data.mapreduce.bean.TopInput;
import com.cl.data.mapreduce.constant.WordSegmentationConstants;
import com.cl.data.mapreduce.util.BuildCsvString;
import com.cl.data.mapreduce.util.DateTimeUtils;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class UserWordCountWeightReducer extends Reducer<Text, TopInput, Text, NullWritable> {

    private static final Set<String> COMMON_HEADER_SET = new HashSet<String>() {{
        add("uid");
        add("mid");
        add("created_at");
        add("weight");
    }};

    @Override
    protected void reduce(Text key, Iterable<TopInput> values, Context context) throws IOException, InterruptedException {
            Map<String, Map<String, CountTime>> groupWordCountMap = new HashMap<>();
            String uid = key.toString();
            Set<String> midSet = new HashSet<>();
            for (TopInput topInput : values) {
                String type = topInput.getType().toString();
                Text value = topInput.getValue();
                try {
                    computeWordCount(midSet, value, groupWordCountMap, WordSegmentationConstants.HEADER_RESULT_KOL_ZONE);
                } catch (Exception e) {
                    log.error("word count error, uid = " + uid + ",type = " + type + ",value=" + value, e);
                }
            }

            String[] header = WordSegmentationConstants.HEADER_RESULT_KOL_ZONE;
            Map<String, Integer> typeIndexMap = getTypeIndexMap(header);
            int size = header.length - 3;
            String[] resultArray = new String[size];
            resultArray[0] = String.valueOf(uid);
            for (int i = 1; i < size; i++) {
                resultArray[i] = "{}";
            }

            for (Map.Entry<String, Map<String, CountTime>> entry : groupWordCountMap.entrySet()) {
                String group = entry.getKey();
                Map<String, CountTime> value = entry.getValue();
                List<Map.Entry<String, CountTime>> list = new LinkedList<>(value.entrySet());
                list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

                Map<String, Float> result = new LinkedHashMap<>();
                for (Map.Entry<String, CountTime> wordCountEntry : list) {
                    result.put(wordCountEntry.getKey(), wordCountEntry.getValue().getCount());
                }
                resultArray[typeIndexMap.get(group)] = JSON.toJSONString(result, SerializerFeature.UseSingleQuotes);
            }
            context.write(new Text(BuildCsvString.build(resultArray, ',')), NullWritable.get());
    }

    private void computeWordCount(Set<String> midSet, Text value, Map<String, Map<String, CountTime>> groupWordCountMap,
                                  String[] header) throws IOException {
        CSVParser parse = CSVParser.parse(value.toString(), CSVFormat.DEFAULT.withHeader(header));
        for (CSVRecord record : parse) {
            if (header.length != record.size()) {
                continue;
            }

            String mid = record.get("mid");
            if (!midSet.add(mid)){
                continue;
            }
            String createdAt = record.get("created_at");
            Timestamp timestamp = DateTimeUtils.transDateTimeStringToTimestamp(createdAt);

            for (String group : header) {
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
                    int countInt = (Integer) count;
                    float weightCount = (float) (2 - Math.pow(0.5, countInt - 1));

                    wordCountMap.merge(word, new CountTime(weightCount, timestamp), (oldCountTime, newCountTime) -> {
                        float wordCount = oldCountTime.getCount() + newCountTime.getCount();
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
        float count;
        Timestamp timestamp;

        CountTime(float count, Timestamp timestamp) {
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

        public float getCount() {
            return count;
        }

        public void setCount(float count) {
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
