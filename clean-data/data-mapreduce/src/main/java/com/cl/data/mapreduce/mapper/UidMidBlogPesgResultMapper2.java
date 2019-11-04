package com.cl.data.mapreduce.mapper;

import com.cl.data.mapreduce.bean.TopInput;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

/**
 * @author yejianyu
 * @date 2019/9/11
 */
@Slf4j
public class UidMidBlogPesgResultMapper2 extends Mapper<LongWritable, Text, Text, TopInput> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        CSVParser parse = CSVParser.parse(value.toString(), CSVFormat.DEFAULT);
        for (CSVRecord record : parse){
            String uid = record.get(0);
            if (!StringUtils.isNumeric(uid)) {
                continue;
            }
            if ("uid".equals(uid)){
                continue;
            }
            String mid = record.get(1);
            if (!StringUtils.isNumeric(mid)){
                continue;
            }
           context.write(new Text(record.get(1)), new TopInput(new Text("pesg"), value));
        }
    }
}
