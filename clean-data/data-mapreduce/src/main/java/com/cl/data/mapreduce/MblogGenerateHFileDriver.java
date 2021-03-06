package com.cl.data.mapreduce;

import com.cl.data.mapreduce.mapper.MblogGenerateHFileMapper;
import com.cl.data.mapreduce.mapper.PesgGenerateHFileMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author yejianyu
 * @date 2019/9/27
 */
public class MblogGenerateHFileDriver extends Configured implements Tool {

    public static void main(String[] args) {
        try {
            int response = ToolRunner.run(HBaseConfiguration.create(), new MblogGenerateHFileDriver(), args);
            if(response == 0) {
                System.out.println("Job is successfully completed...");
            } else {
                System.out.println("Job failed...");
            }
        } catch(Exception exception) {
            exception.printStackTrace();
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        String tableName = "uid_mid_blog_hbase";
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.2.42,192.168.2.45,192.168.2.49");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
        conf.set("hbase.mapreduce.hfileoutputformat.table.name", tableName);
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf(tableName));

        final String inputFile = args[0];
        final String outputFile = args[1];
        final Path outputPath = new Path(outputFile);

        //设置相关类名
        Job job = Job.getInstance(conf, "mblog hbase BulkLoad");
        job.setJarByClass(MblogGenerateHFileDriver.class);
        job.setMapperClass(MblogGenerateHFileMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        //设置文件的输入路径和输出路径
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);
        FileInputFormat.setInputPaths(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, outputPath);
        //配置MapReduce作业，以执行增量加载到给定表中。
        HFileOutputFormat2.configureIncrementalLoad(job, table, conn.getRegionLocator(TableName.valueOf(tableName)));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
