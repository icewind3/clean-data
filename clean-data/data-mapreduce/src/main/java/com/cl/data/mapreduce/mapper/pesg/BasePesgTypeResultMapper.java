package com.cl.data.mapreduce.mapper.pesg;

import com.cl.data.mapreduce.bean.TopInput;
import com.cl.data.mapreduce.constant.WordSegmentationConstants;
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
 * @date 2019/10/9
 */
public class BasePesgTypeResultMapper extends Mapper<LongWritable, Text, Text, TopInput> {

    private static final Set<String> UID_SET = new HashSet<>();

    protected Set<String> getUidSet() {
        return UID_SET;
    }

    protected String getType() {
        return "";
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        URI[] uidUris = Job.getInstance(conf).getCacheFiles();
        if (uidUris == null) {
            return;
        }
        for (URI uidUri : uidUris) {
            Path uidPath = new Path(uidUri.getPath());
            String uidFileName = uidPath.getName();
            initSet(getUidSet(), uidFileName);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        CSVParser parse = CSVParser.parse(value.toString(), CSVFormat.DEFAULT);
        for (CSVRecord record : parse) {
            String uid = record.get(0);
            if (!StringUtils.isNumeric(uid)) {
                continue;
            }
            if ("uid".equals(uid)) {
                continue;
            }
            if (getUidSet().size() > 0 && !getUidSet().contains(uid)) {
                return;
            }
            context.write(new Text(uid), new TopInput(new Text(getType()), value));
        }
    }

    public void initSet(Set<String> set, String fileName) {
        try {
            BufferedReader fis = new BufferedReader(new FileReader(fileName));
            String uid;
            while ((uid = fis.readLine()) != null) {
                set.add(uid);
            }
        } catch (IOException ioe) {
            System.err.println("Caught exception while parsing the cached file '"
                + org.apache.hadoop.util.StringUtils.stringifyException(ioe));
        }
    }
}
