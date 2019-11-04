package com.cl.data.mapreduce.mapper;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
public class UidMidBlogPesgResultMapper extends Mapper<Object, Text, Text, Text> {

    private static final Set<Long> MID_SET = new HashSet<>();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        CSVParser parse = CSVParser.parse(value.toString(), CSVFormat.DEFAULT);
        for (CSVRecord record : parse){
            String uid = record.get(0);
            if ("uid".equals(uid)){
                continue;
            }
            String mid = record.get(1);
            if (MID_SET.contains(Long.parseLong(mid))){
                context.write(new Text(uid), value);
            }
        }
    }

    @Override
    protected void setup(Context context) throws IOException{
        Configuration conf = context.getConfiguration();
        URI[] uidUris = Job.getInstance(conf).getCacheFiles();
        for (URI uidMidUri : uidUris) {
            Path uidMidPath = new Path(uidMidUri.getPath());
            String uidMidFileName = uidMidPath.getName();
            initSet(uidMidFileName);
        }
    }

    private static void initSet(String fileName) {
        try {
            BufferedReader fis = new BufferedReader(new FileReader(fileName));
            String uidMid;
            int lineNum = 0;
            while ((uidMid = fis.readLine()) != null) {
                String[] split = uidMid.split(",");
                MID_SET.add(Long.parseLong(split[1]));
                lineNum++;
                if (lineNum % 100000 == 0){
                    log.info("line={}", lineNum);
                }
            }
        } catch (IOException ioe) {
            System.err.println("Caught exception while parsing the cached file '"
                + org.apache.hadoop.util.StringUtils.stringifyException(ioe));
        }
    }

}
