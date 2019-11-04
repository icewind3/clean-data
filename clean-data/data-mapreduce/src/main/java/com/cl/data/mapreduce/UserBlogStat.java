package com.cl.data.mapreduce;

import com.cl.data.mapreduce.dto.BlogInfoDTO;
import com.cl.data.mapreduce.dto.UserBlogInfoDTO;
import com.cl.data.mapreduce.mapper.UserBlogStatMapper;
import com.cl.data.mapreduce.reducer.UserBlogStatReducer;
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
public class UserBlogStat extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (remainingArgs.length != 4) {
            System.err.println("Usage: UserBlogStat <in> <out> [-uid uidFile]");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "user blog stat");
        job.setJarByClass(UserBlogStat.class);
        job.setMapperClass(UserBlogStatMapper.class);
        job.setReducerClass(UserBlogStatReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BlogInfoDTO.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        List<String> otherArgs = new ArrayList<>();
        for (int i = 0; i < remainingArgs.length; ++i) {
            if ("-uid".equals(remainingArgs[i])) {
                String uidPath = remainingArgs[++i];
                job.addCacheFile(new Path(uidPath + "/" + "uid_blue_v.csv").toUri());
//                job.addCacheFile(new Path(uidPath + "/" + "uid_personal_core.csv").toUri());
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }

        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new UserBlogStat(), args);
        System.exit(run);
    }
}
