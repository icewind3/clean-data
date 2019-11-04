package com.cl.data.mapreduce.practice;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yejianyu
 * @date 2019/10/31
 */
public class ComputeVarianceDriver extends Configured implements Tool {

//    public static void main(String[] args) throws Exception {
//        int[] a = {1, 3, 4, 5, 5, 7, 8, 9, 10, 10, 6, 5, 12, 25, 11, 12, 111, 9, 10, 12, 11};
//        List<Integer> list = new ArrayList<>();
//        int sum = 0;
//        for (int num : a) {
//            sum += num;
//            list.add(num);
//        }
//        int n = list.size();
//        float m = (float) sum / n;
//        double sum2 = 0;
//        for (Integer num : list) {
//            sum2 += Math.pow(m - num, 2);
//        }
//        System.out.println(sum2 / n);
//    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new ComputeVarianceDriver(), args);
        System.exit(run);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Compute Variance");
        job.setJarByClass(ComputeVarianceDriver.class);
        job.setMapperClass(ComputeVarianceMapper.class);
        job.setReducerClass(ComputeVarianceReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));
//        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class ComputeVarianceMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text("num"), value);
        }
    }

    static class ComputeVarianceReducer extends Reducer<Text, Text, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<Integer> list = new ArrayList<>();
            int sum = 0;
            for (Text text : values) {
                String string = text.toString();
                int num = Integer.parseInt(string);
                sum += num;
                list.add(num);
            }
            int n = list.size();
            float m = (float) sum / n;
            double sum2 = 0;
            for (Integer num : list) {
                sum2 += Math.pow(m - num, 2);
            }
            context.write(new Text(String.valueOf(sum2 / n)), NullWritable.get());
        }
    }
}
