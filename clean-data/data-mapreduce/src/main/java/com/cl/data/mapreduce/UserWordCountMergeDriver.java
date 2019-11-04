package com.cl.data.mapreduce;

import com.cl.data.mapreduce.bean.TopInput;
import com.cl.data.mapreduce.mapper.pesg.PesgTypeResultOneMapper;
import com.cl.data.mapreduce.mapper.pesg.PesgTypeResultTwoMapper;
import com.cl.data.mapreduce.reducer.UserWordCountMergeReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author yejianyu
 * @date 2019/8/20
 */
public class UserWordCountMergeDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();

        Job job = Job.getInstance(conf, "word count merge result");
        job.setJarByClass(UserWordCountMergeDriver.class);
        job.setReducerClass(UserWordCountMergeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TopInput.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(100);


        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/yejianyu/output/user_pesg_stat_all3"), TextInputFormat.class, PesgTypeResultOneMapper.class);
        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/yejianyu/output/user_pesg_stat_all_20191017_kol"), TextInputFormat.class, PesgTypeResultTwoMapper.class);
        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/yejianyu/output/user_pesg_stat_all_20191017_fans_1"), TextInputFormat.class, PesgTypeResultTwoMapper.class);
        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/yejianyu/output/user_pesg_stat_all_20191017_fans_second_1"), TextInputFormat.class, PesgTypeResultTwoMapper.class);
        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/yejianyu/output/user_pesg_stat_all_20191017_fans_second_2"), TextInputFormat.class, PesgTypeResultTwoMapper.class);
        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/yejianyu/output/user_pesg_stat_all_20191017_fans_second_3"), TextInputFormat.class, PesgTypeResultTwoMapper.class);
        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/yejianyu/output/user_pesg_stat_all_20191017_fans_second_4"), TextInputFormat.class, PesgTypeResultTwoMapper.class);
        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/yejianyu/output/user_pesg_stat_all_20191017_fans_second_5"), TextInputFormat.class, PesgTypeResultTwoMapper.class);



        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[0]));
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new UserWordCountMergeDriver(), args);
        System.exit(run);
    }
}
