package com.cl.data.mapreduce.mapper;

import com.cl.data.mapreduce.dto.BlogInfoDTO;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.regex.Pattern;

/**
 * @author yejianyu
 * @date 2019/8/20
 */
public class UserBlogStatMapper extends Mapper<Object, Text, Text, BlogInfoDTO> {



    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] values = value.toString().split("~");

        String uid = values[1];
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
}