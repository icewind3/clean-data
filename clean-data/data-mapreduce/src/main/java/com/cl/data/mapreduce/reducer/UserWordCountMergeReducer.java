package com.cl.data.mapreduce.reducer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.cl.data.mapreduce.bean.TopInput;
import com.cl.data.mapreduce.constant.WordSegmentationConstants;
import com.cl.data.mapreduce.constant.WordSegmentationMergeConstants;
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
public class UserWordCountMergeReducer extends Reducer<Text, TopInput, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TopInput> values, Context context) throws IOException, InterruptedException {
        Map<String, Map<String, Integer>> groupWordCountMap = new HashMap<>();
        String uid = key.toString();
        for (TopInput topInput : values) {
            String type = topInput.getType().toString();
            Text value = topInput.getValue();
            try {
                if (WordSegmentationConstants.TYPE_RESULT_1.equals(type)) {
                    computeWordCount(value, groupWordCountMap, WordSegmentationMergeConstants.HEADER_RESULT_MERGE_1);
                } else if (WordSegmentationConstants.TYPE_RESULT_2.equals(type)) {
                    computeWordCount(value, groupWordCountMap, WordSegmentationMergeConstants.HEADER_RESULT_MERGE_2);
                }
            } catch (Exception e) {
                log.error("word count error, uid = " + uid + ",type = " + type + ",value=" + value, e);
            }
        }

        String[] header = WordSegmentationMergeConstants.HEADER_RESULT_MERGE_2;
        Map<String, Integer> typeIndexMap = getTypeIndexMap(header);
        int size = header.length;
        String[] resultArray = new String[size];
        resultArray[0] = String.valueOf(uid);
        for (int i = 1; i < size; i++) {
            resultArray[i] = "{}";
        }

        for (Map.Entry<String, Map<String, Integer>> entry : groupWordCountMap.entrySet()) {
            String group = entry.getKey();
            Map<String, Integer> value = entry.getValue();
            List<Map.Entry<String, Integer>> list = new LinkedList<>(value.entrySet());
            list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

            Map<String, Integer> result = new LinkedHashMap<>();
            for (Map.Entry<String, Integer> wordCountEntry : list) {
                result.put(wordCountEntry.getKey(), wordCountEntry.getValue());
            }
            resultArray[typeIndexMap.get(group)] = JSON.toJSONString(result, SerializerFeature.UseSingleQuotes);
        }
        context.write(new Text(BuildCsvString.build(resultArray, ',')), NullWritable.get());
    }

    private void computeWordCount(Text value, Map<String, Map<String, Integer>> groupWordCountMap, String[] header) throws IOException {
        CSVParser parse = CSVParser.parse(value.toString(), CSVFormat.DEFAULT.withHeader(header));
        for (CSVRecord record : parse) {

            if (header.length != record.size()) {
                continue;
            }

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
                    wordCountMap.merge(word, (Integer) count, Integer::sum);
                });
            }
        }
    }

    private Map<String, Integer> getTypeIndexMap(String[] header) {
        int index = 1;
        Map<String, Integer> map = new HashMap<>();
        for (String group : header) {
            if ("uid".equals(group)) {
                continue;
            }
            map.put(group, index);
            index++;
        }
        return map;
    }
}
