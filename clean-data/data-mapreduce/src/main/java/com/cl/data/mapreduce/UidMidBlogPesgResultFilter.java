package com.cl.data.mapreduce;

import com.cl.data.mapreduce.mapper.AttitudeTopMapper;
import com.cl.data.mapreduce.mapper.CommentTopMapper;
import com.cl.data.mapreduce.mapper.RepostTopMapper;
import com.cl.data.mapreduce.mapper.UidMidBlogPesgResultMapper;
import com.cl.data.mapreduce.reducer.UidMidBlogPesgResultReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yejianyu
 * @date 2019/8/20
 */
public class UidMidBlogPesgResultFilter extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (remainingArgs.length != 4) {
            System.err.println("Usage: UidMidBlogPesgResultFilter <in> <out> [-uidMid uidMidFile]");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "pesg result filter");
        job.setJarByClass(UidMidBlogPesgResultFilter.class);
        job.setMapperClass(UidMidBlogPesgResultMapper.class);
        job.setReducerClass(UidMidBlogPesgResultReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(30);
        List<String> otherArgs = new ArrayList<>();
        for (int i = 0; i < remainingArgs.length; ++i) {
            if ("-uidMid".equals(remainingArgs[i])) {
                String uidPath = remainingArgs[++i];
                job.addCacheFile(new Path(uidPath).toUri());
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }

        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/yejianyu/output/user_blog_top10/uid_mid_top10_attitude/data-r-00000"), TextInputFormat.class, AttitudeTopMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/yejianyu/output/user_blog_top10/uid_mid_top10_comment/data-r-00000"), TextInputFormat.class, CommentTopMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/yejianyu/output/user_blog_top10/uid_mid_top10_repost/data-r-00000"), TextInputFormat.class, RepostTopMapper.class);
//        MultipleInputs.addInputPath(job, new Path(otherArgs.get(0)), TextInputFormat.class, UidMidBlogPesgResultMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new UidMidBlogPesgResultFilter(), args);
        System.exit(run);
    }
}
