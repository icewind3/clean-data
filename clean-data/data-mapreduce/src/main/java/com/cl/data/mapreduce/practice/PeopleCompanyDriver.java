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
import java.util.ArrayList;
import java.util.List;

/**
 * @author yejianyu
 * @date 2019/10/31
 */
public class PeopleCompanyDriver extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new PeopleCompanyDriver(), args);
        System.exit(run);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "People Company");
        job.setJarByClass(PeopleCompanyDriver.class);
        job.setMapperClass(PeopleCompanyMapper.class);
        job.setReducerClass(PeopleCompanyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class PeopleCompanyMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString();
            String[] split = s.split(",");
            if (StringUtils.isNumeric(split[0])) {
                context.write(new Text("people"), new Text(split[2] + "," + split[1]));
                context.write(new Text("mobile"), new Text(split[3]));
                context.write(new Text("company"), new Text(split[4]));
            }
        }
    }

    static class PeopleCompanyReducer extends Reducer<Text, Text, Text, NullWritable> {

        private MultipleOutputs<Text, NullWritable> multipleOutputs;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String type = key.toString();
            String path = null;
            String header = "";
            switch (type) {
                case "people":
                    path = "people/data";
                    header = "IDCARD,NAME";
                    break;
                case "mobile":
                    path = "mobile/data";
                    header = "MOBILE";
                    break;
                case "company":
                    path = "company/data";
                    header = "COMPANY";
                    break;
                default:
                    break;
            }
            if (StringUtils.isBlank(path)){
                return;
            }
            multipleOutputs.write(new Text(header), NullWritable.get(), path);
            for (Text value : values) {
                if (StringUtils.isNotBlank(value.toString())){
                    multipleOutputs.write(value, NullWritable.get(), path);
                }
            }
        }

        @Override
        protected void setup(Context context) {
            multipleOutputs = new MultipleOutputs<>(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }
}
