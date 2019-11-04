package com.cl.data.mapreduce.mapper;

import com.alibaba.fastjson.JSON;
import com.cl.data.mapreduce.util.FileUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CategoryStatisticMapper extends Mapper<LongWritable, Text, Text, Text> {

    private int columnCount;
    private static final Log logger = LogFactory.getLog(CategoryStatisticMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        columnCount = context.getConfiguration().getInt("column.count", 0);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] array = FileUtil.translateCsvValues(value).toArray(new String[0]);
        context.write(new Text(array[0]), new Text(JSON.toJSONString(array)));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
