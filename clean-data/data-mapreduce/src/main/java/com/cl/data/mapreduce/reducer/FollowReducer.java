package com.cl.data.mapreduce.reducer;

import com.cl.data.mapreduce.bean.FollowRelationship;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * @author yejianyu
 * @date 2019/8/20
 */
public class FollowReducer extends Reducer<Text, FollowRelationship, NullWritable, FollowRelationship> {

    private MultipleOutputs<NullWritable, FollowRelationship> multipleOutputs;

    @Override
    protected void setup(Context context) {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<FollowRelationship> values, Context context)
            throws IOException, InterruptedException {
        for (FollowRelationship value : values) {
            multipleOutputs.write(NullWritable.get(), value, key.toString() + "/" + key.toString());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}