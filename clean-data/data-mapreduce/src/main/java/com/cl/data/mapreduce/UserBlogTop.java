package com.cl.data.mapreduce;

import com.cl.data.mapreduce.dto.BlogInfoDTO;
import com.cl.data.mapreduce.mapper.UserBlogTopMapper;
import com.cl.data.mapreduce.reducer.UserBlogTopPercentReducer;
import com.cl.data.mapreduce.reducer.UserBlogTopReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yejianyu
 * @date 2019/8/20
 */
public class UserBlogTop extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (remainingArgs.length != 4) {
            System.err.println("Usage: UserBlogTop <in> <out> [-uid uidFile]");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "user blog top");
        job.setJarByClass(UserBlogTop.class);
        job.setMapperClass(UserBlogTopMapper.class);
//        job.setReducerClass(UserBlogTopReducer.class);
        job.setReducerClass(UserBlogTopPercentReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BlogInfoDTO.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(100);
        List<String> otherArgs = new ArrayList<>();
        for (int i = 0; i < remainingArgs.length; ++i) {
            if ("-uid".equals(remainingArgs[i])) {
                String uidPath = remainingArgs[++i];
                job.addCacheFile(new Path(uidPath).toUri());
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }

        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new UserBlogTop(), args);
        System.exit(run);
    }
}
