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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author yejianyu
 * @date 2019/10/31
 */
public class ReplaceValueDriver extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new ReplaceValueDriver(), args);
        System.exit(run);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();

        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, strings);
        String[] remainingArgs = optionsParser.getRemainingArgs();


        Job job = Job.getInstance(conf, "Replace  Value");
        job.setJarByClass(ReplaceValueDriver.class);
        job.setMapperClass(ReplaceValueMapper.class);
        job.setReducerClass(ReplaceValueReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.getConfiguration().set("column", remainingArgs[2]);
        job.getConfiguration().set("value", remainingArgs[3]);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class ReplaceValueMapper extends Mapper<LongWritable, Text, Text, Text> {

        private static Map<String, Integer> map = new HashMap<>();
        static {
            map.put("APP_ID", 0);
            map.put("SEX", 1);
            map.put("DEBIT", 2);
            map.put("SOURCE", 3);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0){
                return;
            }
            String column = context.getConfiguration().get("column");
            String replaceValue = context.getConfiguration().get("value");
            int replaceIndex = map.get(column);
            String s = value.toString();
            String[] split = s.split(",");
            String oriValue = split[replaceIndex];
            String[] replaceArray = replaceValue.split(",");
            for (String replace : replaceArray){
                if (replace.equals(oriValue)){
                    split[replaceIndex] = "";
                    break;
                }
            }
            StringBuilder result = new StringBuilder();
            for (String item : split) {
                result.append(item).append(",");
            }
            context.write(new Text("result"), new Text(result.substring(0, result.length() - 1)));
        }
    }

    static class ReplaceValueReducer extends Reducer<Text, Text, Text, NullWritable> {


        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String header = "APP_ID,SEX,DEBIT,SOURCE";
            context.write(new Text(header), NullWritable.get());
            for (Text text : values){
                context.write(text, NullWritable.get());
            }
        }

    }
}
