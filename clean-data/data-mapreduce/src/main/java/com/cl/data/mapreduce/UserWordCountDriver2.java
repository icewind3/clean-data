package com.cl.data.mapreduce;

import com.cl.data.mapreduce.bean.TopInput;
import com.cl.data.mapreduce.mapper.pesg.*;
import com.cl.data.mapreduce.reducer.UserWordCountMultiReducer;
import com.cl.data.mapreduce.reducer.UserWordCountWeightReducer;
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
public class UserWordCountDriver2 extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();

        Job job = Job.getInstance(conf, "word count2 multi result");
        job.setJarByClass(UserWordCountDriver2.class);
//        job.setReducerClass(UserWordCountMultiReducer.class);
        job.setReducerClass(UserWordCountWeightReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TopInput.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(100);

//        job.addCacheFile(new Path("/user/clhadoop/yejianyu/input/uid_220w.csv").toUri());

        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/yejianyu/uid_mid_blog_pesg_kol_zone_results"), TextInputFormat.class, PesgTypeResultKolZoneMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/uid_mid_blog_pesg_merge"), TextInputFormat.class, PesgTypeResultOneMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/uid_mid_blog_pesg_merge2"), TextInputFormat.class, PesgTypeResultTwoMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/uid_mid_blog_pesg_merge3"), TextInputFormat.class, PesgTypeResultThreeMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/uid_mid_blog_pesg_new15w"), TextInputFormat.class, PesgTypeResultNewKolMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/uid_mid_blog_pesg_new15w_2"), TextInputFormat.class, PesgTypeResultNewKolTwoMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/uid_mid_blog_pesg_80w_kol"), TextInputFormat.class, PesgTypeResult80wKolMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/uid_mid_blog_pesg_fans_merge/fans_result"), TextInputFormat.class, PesgTypeResultFansMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/uid_mid_blog_pesg_fans_merge/fans_second_result"), TextInputFormat.class, PesgTypeResultFansSecondMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/uid_mid_blog_pesg_fans_merge/fans_second_result2"), TextInputFormat.class, PesgTypeResultFansSecondMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/uid_mid_blog_pesg_fans_merge/fans_second_result3"), TextInputFormat.class, PesgTypeResultFansSecondMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/uid_mid_blog_pesg_fans_merge/fans_second_result4"), TextInputFormat.class, PesgTypeResultFansSecondMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/uid_mid_blog_pesg_fans_merge/fans_second_result5"), TextInputFormat.class, PesgTypeResultFansSecondMapper.class);

//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/uid_mid_blog_pesg_merge/uid_mid_blog_100_result.csv"), TextInputFormat.class, PesgTypeResultOneMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/uid_mid_blog_pesg_merge2/uid_mid_blog_100_result.csv"), TextInputFormat.class, PesgTypeResultTwoMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/uid_mid_blog_pesg_merge3/uid_mid_blog_100_result.csv"), TextInputFormat.class, PesgTypeResultThreeMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/uid_mid_blog_pesg_new15w/uid_mid_blog_365_result.csv"), TextInputFormat.class, PesgTypeResultNewKolMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/uid_mid_blog_pesg_new15w_2/uid_mid_blog_365_result.csv"), TextInputFormat.class, PesgTypeResultNewKolTwoMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/uid_mid_blog_pesg_80w_kol/uid_mid_blog_80w_kol_100_result.csv"), TextInputFormat.class, PesgTypeResult80wKolMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/uid_mid_blog_pesg_fans_merge/fans_result/uid_mid_blog_389_result.csv"), TextInputFormat.class, PesgTypeResultFansMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/uid_mid_blog_pesg_fans_merge/fans_second_result/uid_mid_blog_1041_result.csv"), TextInputFormat.class, PesgTypeResultFansSecondMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/uid_mid_blog_pesg_fans_merge/fans_second_result2/uid_mid_blog_1000_result.csv"), TextInputFormat.class, PesgTypeResultFansSecondMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/uid_mid_blog_pesg_fans_merge/fans_second_result3/uid_mid_blog_1121_result.csv"), TextInputFormat.class, PesgTypeResultFansSecondMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/uid_mid_blog_pesg_fans_merge/fans_second_result4/uid_mid_blog_1011_result.csv"), TextInputFormat.class, PesgTypeResultFansSecondMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/uid_mid_blog_pesg_fans_merge/fans_second_result5/uid_mid_blog_1001_result.csv"), TextInputFormat.class, PesgTypeResultFansSecondMapper.class);

        String fileName = "user_pesg_repost";
        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/yejianyu/output/user_blog_top50_stat_kol_zone_20191104/" + fileName), TextInputFormat.class, PesgTypeResultKolZoneMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/yejianyu/output/user_blog_median_stat/user_blog_median30_pesg1/" + fileName), TextInputFormat.class, PesgTypeResultOneMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/yejianyu/output/user_blog_median_stat/user_blog_median30_pesg2/" + fileName), TextInputFormat.class, PesgTypeResultTwoMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/yejianyu/output/user_blog_median_stat/user_blog_median30_pesg3/" + fileName), TextInputFormat.class, PesgTypeResultThreeMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/yejianyu/output/user_blog_median_stat/user_blog_median30_pesg_80w_kol/" + fileName), TextInputFormat.class, PesgTypeResultNewKolMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/yejianyu/output/user_blog_median_stat/user_blog_median30_pesg_new15w/" + fileName), TextInputFormat.class, PesgTypeResultNewKolTwoMapper.class);
//        MultipleInputs.addInputPath(job, new Path("/user/clhadoop/yejianyu/output/user_blog_median_stat/user_blog_median30_pesg_new15w_2/" + fileName), TextInputFormat.class, PesgTypeResult80wKolMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[0]));
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new UserWordCountDriver2(), args);
        System.exit(run);
    }
}
