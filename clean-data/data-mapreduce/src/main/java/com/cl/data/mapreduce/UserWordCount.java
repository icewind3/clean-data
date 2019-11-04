package com.cl.data.mapreduce;

import com.cl.data.mapreduce.mapper.UserWordCountMapper;
import com.cl.data.mapreduce.reducer.UserWordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yejianyu
 * @date 2019/8/20
 */
public class UserWordCount extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (remainingArgs.length != 4) {
            System.err.println("Usage: UserWordCount <in> <out> [-uid uidFile]");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "user word count");
        job.setJarByClass(UserWordCount.class);
        job.setMapperClass(UserWordCountMapper.class);
        job.setReducerClass(UserWordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

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

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new UserWordCount(), args);
        System.exit(run);
    }
}
