package com.cl.data.mapreduce.mapper;

import com.cl.data.mapreduce.dto.BlogInfoDTO;
import com.cl.data.mapreduce.util.UserTypeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

/**
 * @author yejianyu
 * @date 2019/8/20
 */
public class UserBlogTopMapper extends Mapper<Object, Text, Text, BlogInfoDTO> {

    private static final Set<String> UID_SET = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        URI[] uidUris = Job.getInstance(conf).getCacheFiles();
        for (URI uidUri : uidUris) {
            Path uidPath = new Path(uidUri.getPath());
            String uidFileName = uidPath.getName();
            initSet(UID_SET, uidFileName);
        }
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] values = value.toString().split("~");

        String uid = values[1];
        if (!UID_SET.contains(uid)){
            return;
        }
        String mid = values[0];
        long repostsCount = Long.parseLong(values[2]);
        long commentsCount = Long.parseLong(values[3]);
        long attitudesCount = Long.parseLong(values[4]);
        String text = values[5];
        String retweetedText = values[6];
        String createdAt = values[7];
        BlogInfoDTO blogInfoDTO = new BlogInfoDTO(new Text(uid));
        blogInfoDTO.setMid(new Text(mid));
        blogInfoDTO.setRepostCount(new LongWritable(repostsCount));
        blogInfoDTO.setCommentCount(new LongWritable(commentsCount));
        blogInfoDTO.setAttitudeCount(new LongWritable(attitudesCount));
        blogInfoDTO.setCreatedAt(new Text(createdAt));
        if (StringUtils.isNotBlank(retweetedText) || StringUtils.isBlank(text)){
            blogInfoDTO.setIsRetweeted(new IntWritable(1));
        } else {
            blogInfoDTO.setIsRetweeted(new IntWritable(0));
        }
        context.write(new Text(uid), blogInfoDTO);
    }

    private static void initSet(Set<String> set, String fileName) {
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