package com.cl.data.mapreduce.dto;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author yejianyu
 * @date 2019/8/9
 */
public class BlogInfoDTO implements Writable {

    private Text uid;
    private Text mid;
    private LongWritable attitudeCount;
    private LongWritable commentCount;
    private LongWritable repostCount;
    private Text createdAt;
    private IntWritable isRetweeted;

    public BlogInfoDTO() {
    }

    public BlogInfoDTO(Text uid) {
        this.uid = uid;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        uid.write(dataOutput);
        mid.write(dataOutput);
        attitudeCount.write(dataOutput);
        commentCount.write(dataOutput);
        repostCount.write(dataOutput);
        createdAt.write(dataOutput);
        isRetweeted.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        uid = new Text();
        uid.readFields(dataInput);
        mid = new Text();
        mid.readFields(dataInput);
        attitudeCount = new LongWritable();
        attitudeCount.readFields(dataInput);
        commentCount = new LongWritable();
        commentCount.readFields(dataInput);
        repostCount = new LongWritable();
        repostCount.readFields(dataInput);
        createdAt = new Text();
        createdAt.readFields(dataInput);
        isRetweeted = new IntWritable();
        isRetweeted.readFields(dataInput);
    }

    public Text getUid() {
        return uid;
    }

    public void setUid(Text uid) {
        this.uid = uid;
    }

    public Text getMid() {
        return mid;
    }

    public void setMid(Text mid) {
        this.mid = mid;
    }

    public LongWritable getAttitudeCount() {
        return attitudeCount;
    }

    public void setAttitudeCount(LongWritable attitudeCount) {
        this.attitudeCount = attitudeCount;
    }

    public LongWritable getCommentCount() {
        return commentCount;
    }

    public void setCommentCount(LongWritable commentCount) {
        this.commentCount = commentCount;
    }

    public LongWritable getRepostCount() {
        return repostCount;
    }

    public void setRepostCount(LongWritable repostCount) {
        this.repostCount = repostCount;
    }

    public Text getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Text createdAt) {
        this.createdAt = createdAt;
    }

    public IntWritable getIsRetweeted() {
        return isRetweeted;
    }

    public void setIsRetweeted(IntWritable isRetweeted) {
        this.isRetweeted = isRetweeted;
    }
}
