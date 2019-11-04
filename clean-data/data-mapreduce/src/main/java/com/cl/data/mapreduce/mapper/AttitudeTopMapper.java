package com.cl.data.mapreduce.mapper;

import com.cl.data.mapreduce.bean.TopInput;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yejianyu
 * @date 2019/9/24
 */
public class AttitudeTopMapper extends Mapper<LongWritable, Text, Text, TopInput> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        context.write(new Text(split[1]), new TopInput(new Text("attitude"), new Text()));
    }
}
