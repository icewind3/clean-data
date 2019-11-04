package com.cl.data.mapreduce.mapper;

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
import java.rmi.server.UID;
import java.util.HashSet;
import java.util.Set;

/**
 * @author yejianyu
 * @date 2019/9/11
 */
public class UserWordCountMapper extends Mapper<Object, Text, Text, Text> {

    private static final Set<String> UID_SET = new HashSet<>();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        CSVParser parse = CSVParser.parse(value.toString(), CSVFormat.DEFAULT);
        for (CSVRecord record : parse){
            String uid = record.get(0);
            if ("uid".equals(uid)){
                continue;
            }
            if (UID_SET.contains(uid)){
                context.write(new Text(uid), value);
            }
        }
    }

    @Override
    protected void setup(Context context) throws IOException{
        Configuration conf = context.getConfiguration();
        URI[] uidUris = Job.getInstance(conf).getCacheFiles();
        for (URI uidUri : uidUris) {
            Path uidMidPath = new Path(uidUri.getPath());
            String uidFileName = uidMidPath.getName();
            initSet(uidFileName);
        }
    }

    private static void initSet(String fileName) {
        try {
            BufferedReader fis = new BufferedReader(new FileReader(fileName));
            String uid;
            while ((uid = fis.readLine()) != null) {
                UID_SET.add(uid.trim());
            }
        } catch (IOException ioe) {
            System.err.println("Caught exception while parsing the cached file '"
                + org.apache.hadoop.util.StringUtils.stringifyException(ioe));
        }
    }

}
