package com.cl.data.mapreduce.reducer;

import com.cl.data.mapreduce.bean.TopInput;
import com.cl.data.mapreduce.util.GsonUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yejianyu
 * @date 2019/9/11
 */
public class UidMidBlogPesgResultReducer2 extends Reducer<Text, TopInput, Text, NullWritable> {

    private MultipleOutputs<Text, NullWritable> multipleOutputs;

    @Override
    protected void reduce(Text key, Iterable<TopInput> values, Context context) throws IOException, InterruptedException {
        List<String> topInputList = new ArrayList<>();
        for (TopInput topInput : values){
            topInputList.add(GsonUtil.getGson().toJson(topInput));
        }
        List<String> pathList = new ArrayList<>();
        Text text = null;
        if (topInputList.size() >= 2){
            for (String temp : topInputList){
                TopInput topInput = GsonUtil.getGson().fromJson(temp, TopInput.class);
                String type = topInput.getType().toString();
                if ("pesg".equals(type)){
                    text = topInput.getValue();
                }
                if ("repost".equals(type)) {
                    pathList.add("user_pesg_repost/data");
                }
                if ("comment".equals(type)) {
                    pathList.add("user_pesg_comment/data");
                }
                if ("attitude".equals(type)) {
                    pathList.add("user_pesg_attitude/data");
                }
            }
            if (text == null){
                return;
            }

            for (String path : pathList){
                multipleOutputs.write(text, NullWritable.get(), path);
            }
        }
    }

    @Override
    protected void setup(Context context) throws IOException {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
