package com.cl.data.mapreduce;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.cl.data.mapreduce.constant.WordSegmentationConstants;
import com.cl.data.mapreduce.reducer.UserWordCountMultiReducer;
import com.cl.data.mapreduce.util.BuildCsvString;
import com.cl.data.mapreduce.util.DateTimeUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;

/**
 * @author yejianyu
 * @date 2019/10/16
 */
public class HbasePesgStatDriver extends Configured implements Tool {


    private static final Set<String> COMMON_HEADER_SET = new HashSet<String>() {{
        add("uid");
        add("mid");
        add("created_at");
        add("weight");
    }};

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new HbasePesgStatDriver(), args);
        System.exit(run);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration  config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "192.168.2.42,192.168.2.45,192.168.2.49");
        config.set("zookeeper.znode.parent", "/hbase-unsecure");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.rpc.timeout", "60000");
        Job job = Job.getInstance(config, "hbase word count");
        job.setJarByClass(HbasePesgStatDriver.class); // class that contains mapper and reducer
        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
//        scan.setTimeRange(1400468400000L, 3544178846310758L);
        scan.addFamily(Bytes.toBytes("result1"));
        scan.setMaxVersions();

        TableMapReduceUtil.initTableMapperJob(
            "mblog_from_uid_merge_pesg3",        // input HBase table name
            scan,             // Scan instance to control CF and attribute selection
            HbasePesgStatMapper.class,   // mapper
            Text.class,             // mapper output key
            NullWritable.class,             // mapper output value
            job);
////        job.setReducerClass();
//        job.setNumReduceTasks(100);
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class HbasePesgStatMapper extends TableMapper<Text, NullWritable> {

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            Map<String, Map<String, Integer>> groupWordCountMap = new HashMap<>();
            String uid = Bytes.toString(key.get());
            Cell[] cells = value.rawCells();
            for (Cell cell : cells) {
                String group = Bytes.toString(CellUtil.cloneQualifier(cell));
                String keyword = Bytes.toString(CellUtil.cloneValue(cell));
                computeWordCount(group, keyword, groupWordCountMap);
            }
            String[] header = WordSegmentationConstants.HEADER_RESULT_FANS_SECOND;
            Map<String, Integer> typeIndexMap = getTypeIndexMap(header);
            int size = header.length - 3;
            String[] resultArray = new String[size];
            resultArray[0] = String.valueOf(uid);
            for (int i = 1; i < size; i++) {
                resultArray[i] = "{}";
            }

            for (Map.Entry<String, Map<String, Integer>> entry : groupWordCountMap.entrySet()) {
                String group = entry.getKey();
                Map<String, Integer> wordCountMap = entry.getValue();
                List<Map.Entry<String, Integer>> list = new LinkedList<>(wordCountMap.entrySet());
                list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

                Map<String, Integer> result = new LinkedHashMap<>();
                for (Map.Entry<String, Integer> wordCountEntry : list) {
                    result.put(wordCountEntry.getKey(), wordCountEntry.getValue());
                }
               Integer index = typeIndexMap.get(group);
                if (index != null){
                    resultArray[index] = JSON.toJSONString(result, SerializerFeature.UseSingleQuotes);
                }
            }
            context.write(new Text(BuildCsvString.build(resultArray, ',')), NullWritable.get());
        }
    }

    private static void computeWordCount(String group, String keyword, Map<String, Map<String, Integer>> groupWordCountMap) throws IOException {
        if (COMMON_HEADER_SET.contains(group)) {
            return;
        }
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

    private static Map<String, Integer> getTypeIndexMap(String[] header) {
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

}
