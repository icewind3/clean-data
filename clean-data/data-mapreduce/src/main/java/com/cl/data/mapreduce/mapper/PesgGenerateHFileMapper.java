package com.cl.data.mapreduce.mapper;

import com.cl.data.mapreduce.constant.WordSegmentationConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author yejianyu
 * @date 2019/9/27
 */
@Slf4j
public class PesgGenerateHFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {


    private static final String WORD_DEFAULT_VALUE = "{}";

    private static final Set<String> COMMON_HEADER_SET = new HashSet<String>() {{
        add("uid");
        add("mid");
        add("created_at");
        add("weight");
    }};

    @Override
    protected void map(LongWritable key, Text value,
                       Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
        throws IOException, InterruptedException {
        String[] header = WordSegmentationConstants.HEADER_RESULT_FANS_SECOND;
        CSVParser parse = CSVParser.parse(value.toString(), CSVFormat.DEFAULT.withHeader(header));
        for (CSVRecord record : parse) {
            if (record.size() != header.length) {
                continue;
            }
            String uid = record.get(0);
            if ("uid".equals(uid)) {
                continue;
            }
            String mid = record.get(1);

            if (StringUtils.isBlank(uid) || StringUtils.isBlank(mid)) {
                continue;
            }
            try {
                Long.parseLong(uid);
                Long.parseLong(mid);
            } catch (Exception e) {
                log.error("word count data error, uid={}, mid={}, value={}", uid, mid, value.toString());
                continue;
            }

            String columnFamily = "result1";
            Map<String, String> typeWordMap = getTypeWordMap(header, record);

            if (typeWordMap.isEmpty()) {
                continue;
            }

            //拼装rowkey和put
            ImmutableBytesWritable putRowKey = new ImmutableBytesWritable(uid.getBytes());
            Put put = createPut(columnFamily, uid, mid, typeWordMap);
            String createdAt = record.get("created_at");
            if (StringUtils.isNotBlank(createdAt)) {
                addColumn(put, columnFamily, "created_at", Long.parseLong(mid), createdAt, StringUtils.EMPTY);
            }
            context.write(putRowKey, put);
        }
    }

    private Put createPut(String family, String uid, String mid, Map<String, String> typeWordMap) {

        long ts = Long.parseLong(mid);
        Put put = new Put(Bytes.toBytes(uid));
        typeWordMap.forEach((key, value) -> {
            addColumn(put, family, key, ts, value, WORD_DEFAULT_VALUE);
        });
        return put;
    }

    private void addColumn(Put put, String family, String qualifier, long ts, Object value, String defaultValue) {
        String realValue;
        if (value == null) {
            realValue = defaultValue;
        } else {
            realValue = String.valueOf(value);
        }
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), ts, Bytes.toBytes(realValue));
    }

    private Map<String, String> getTypeWordMap(String[] header, CSVRecord record) {
        Map<String, String> map = new HashMap<>();
        for (String key : header) {
            if (!COMMON_HEADER_SET.contains(key)) {
                String wordResult = record.get(key);
                if (!WORD_DEFAULT_VALUE.equals(wordResult)) {
                    map.put(key, wordResult);
                }
            }
        }
        return map;
    }
}
