package com.cl.data.mapreduce.practice;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * @author yejianyu
 * @date 2019/10/31
 */
public class UserConsumeDriver extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new UserConsumeDriver(), args);
        System.exit(run);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "User Consume");
        job.setJarByClass(UserConsumeDriver.class);
        job.setMapperClass(UserConsumeMapper.class);
        job.setReducerClass(UserConsumeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class UserConsumeMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString();
            String[] split = s.split(",");
            context.write(new Text(split[0]), new Text(split[2]));
        }

    }

    static class UserConsumeReducer extends Reducer<Text, Text, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String uid = key.toString();
            float sum = 0;
            for (Text value : values) {
                sum += Float.parseFloat(value.toString());
            }
            context.write(new Text(uid + "," + (sum / 4)), NullWritable.get());
        }
    }

    static class UserConsumeMapper2 extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text("result"), value);
        }
    }

    static class UserConsumeReducer2 extends Reducer<Text, Text, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<UidCost> list = new ArrayList<>();
            for (Text value : values) {

//                sum += Float.parseFloat(value.toString());
            }
//            context.write(new Text(uid + "," + (sum / 4)), NullWritable.get());
        }

        class UidCost implements Comparable<UidCost>{
            String uid;
            Float cost;


            @Override
            public int compareTo(UidCost o) {
                return this.cost.compareTo(o.cost);
            }
        }
    }

}
