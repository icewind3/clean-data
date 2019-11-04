package com.cl.data.mapreduce;

import com.cl.data.mapreduce.mapper.HbaseMblogStatMapper;
import com.cl.data.mapreduce.mapper.HbaseMblogWeekStatMapper;
import com.cl.data.mapreduce.util.CsvFileHelper;
import com.csvreader.CsvReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapTask;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @author yejianyu
 * @date 2019/10/16
 */
@Slf4j
public class HbaseMblogStatDriver extends Configured implements Tool {

    private static final String BLOG_HBASE_FAMILY_WEIBO = "weibo";
    private static final String BLOG_HBASE_QUALIFIER_CREATED_AT = "created_at";
    private static final String BLOG_HBASE_QUALIFIER_MID = "mid";
    private static final String BLOG_HBASE_QUALIFIER_IS_RETWEETED = "is_retweeted";
    private static final String BLOG_HBASE_QUALIFIER_ATTITUDES_COUNT = "attitudes_count";
    private static final String BLOG_HBASE_QUALIFIER_COMMENTS_COUNT = "comments_count";
    private static final String BLOG_HBASE_QUALIFIER_REPOSTS_COUNT = "reposts_count";

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new HbaseMblogStatDriver(), args);
        System.exit(run);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();

//        config.set("hbase.zookeeper.quorum", "192.168.2.45");
        config.set("hbase.zookeeper.quorum", "192.168.2.42,192.168.2.45,192.168.2.49");
        config.set("zookeeper.znode.parent", "/hbase-unsecure");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 600000);
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 600000);
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 600000);
//        config.setInt(HConstants.HBASE_RPC_SHORTOPERATION_TIMEOUT_KEY, 300000);

//        config.set(HConstants.HBASE_CLIENT_SCANNER_CACHING, "5");
        Job job = Job.getInstance(config, "hbase mblog stat");
        job.setJarByClass(HbaseMblogStatDriver.class); // class that contains mapper and reducer

        String tableName = "uid_mid_blog_hbase";
        byte[] family = Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO);

//        Scan scan = new Scan();
//        scan.setCaching(2);        // 1 is the default in Scan, which will be bad for MapReduce jobs
//        scan.setCacheBlocks(false);  // don't set to true for MR jobs

//        scan.addFamily(family);
//        scan.addColumn(family, Bytes.toBytes(BLOG_HBASE_QUALIFIER_CREATED_AT));
//        scan.addColumn(family, Bytes.toBytes(BLOG_HBASE_QUALIFIER_IS_RETWEETED));
//        scan.addColumn(family, Bytes.toBytes(BLOG_HBASE_QUALIFIER_ATTITUDES_COUNT));
//        scan.addColumn(family, Bytes.toBytes(BLOG_HBASE_QUALIFIER_COMMENTS_COUNT));
//        scan.addColumn(family, Bytes.toBytes(BLOG_HBASE_QUALIFIER_REPOSTS_COUNT));
//        scan.setStartRow(Bytes.toBytes("2"));
//        scan.setStopRow(Bytes.toBytes("25"));
//        scan.setMaxVersions();
//
//        TableMapReduceUtil.initTableMapperJob(
//            tableName,        // input HBase table name
//            scan,             // Scan instance to control CF and attribute selection
//            HbaseMblogWeekStatMapper.class,   // mapper
//            Text.class,             // mapper output key
//            NullWritable.class,             // mapper output value
//            job);

//        String[] rowArray = {"25", "26","27","28","29","3"};
        String[] rowArray = {"2", "21","22","23","24","25"};
        List<Scan> scans = new ArrayList<>();
        for (int i = 0; i < rowArray.length - 1; i++) {
            Scan scan = new Scan();
            scan.setCaching(2);        // 1 is the default in Scan, which will be bad for MapReduce jobs
            scan.setCacheBlocks(false);  // don't set to true for MR jobs

            scan.addFamily(family);
            scan.addColumn(family, Bytes.toBytes(BLOG_HBASE_QUALIFIER_CREATED_AT));
            scan.addColumn(family, Bytes.toBytes(BLOG_HBASE_QUALIFIER_IS_RETWEETED));
            scan.addColumn(family, Bytes.toBytes(BLOG_HBASE_QUALIFIER_ATTITUDES_COUNT));
            scan.addColumn(family, Bytes.toBytes(BLOG_HBASE_QUALIFIER_COMMENTS_COUNT));
            scan.addColumn(family, Bytes.toBytes(BLOG_HBASE_QUALIFIER_REPOSTS_COUNT));
            scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,  Bytes.toBytes(tableName));

            scan.setStartRow(Bytes.toBytes(rowArray[i]));
            scan.setStopRow(Bytes.toBytes(rowArray[i+1]));
            scan.setMaxVersions();
            scans.add(scan);
        }

//        List<Scan> scans = genScanList();

        TableMapReduceUtil.initTableMapperJob(
            scans,             // Scan instance to control CF and attribute selection
            HbaseMblogWeekStatMapper.class,   // mapper
            Text.class,             // mapper output key
            NullWritable.class,             // mapper output value
            job);

        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private List<Scan> genScanList() {
        List<Scan> scanList = new ArrayList<>(500010);
        byte[] family = Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO);
        String tableName = "uid_mid_blog_hbase";
        String filePath = "/data6_1/user_blog_top/uid_220w_0.csv";
        try (CSVParser csvParser = CsvFileHelper.reader(filePath)) {
            for (CSVRecord record : csvParser) {
                String uid = record.get(0);
                Scan scan = new Scan();
                scan.setCaching(2);        // 1 is the default in Scan, which will be bad for MapReduce jobs
                scan.setCacheBlocks(false);  // don't set to true for MR jobs

                scan.addFamily(family);
                scan.addColumn(family, Bytes.toBytes(BLOG_HBASE_QUALIFIER_CREATED_AT));
                scan.addColumn(family, Bytes.toBytes(BLOG_HBASE_QUALIFIER_IS_RETWEETED));
                scan.addColumn(family, Bytes.toBytes(BLOG_HBASE_QUALIFIER_ATTITUDES_COUNT));
                scan.addColumn(family, Bytes.toBytes(BLOG_HBASE_QUALIFIER_COMMENTS_COUNT));
                scan.addColumn(family, Bytes.toBytes(BLOG_HBASE_QUALIFIER_REPOSTS_COUNT));
                scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(tableName));

                scan.setStartRow(Bytes.toBytes(uid));
                scan.setStopRow(Bytes.toBytes(uid));
                scan.setMaxVersions();
                scanList.add(scan);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return scanList;
    }


}
