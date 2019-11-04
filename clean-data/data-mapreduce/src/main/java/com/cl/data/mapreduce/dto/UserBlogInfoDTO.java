package com.cl.data.mapreduce.dto;

import lombok.Data;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Timestamp;

/**
 * @author yejianyu
 * @date 2019/8/9
 */
public class UserBlogInfoDTO implements Writable {

    private Text uid;
    private FloatWritable releaseFrequency;
    private LongWritable attitudeAvg;
    private LongWritable commentAvg;
    private LongWritable repostAvg;
    private LongWritable attitudeMedian;
    private LongWritable commentMedian;
    private LongWritable repostMedian;
    private FloatWritable repostRate;

    private FloatWritable releaseFrequency2;
    private LongWritable attitudeAvg2;
    private LongWritable commentAvg2;
    private LongWritable repostAvg2;
    private LongWritable attitudeMedian2;
    private LongWritable commentMedian2;
    private LongWritable repostMedian2;
    private FloatWritable repostRate2;


    private LongWritable attitudeTopAvg;
    private LongWritable commentTopAvg;
    private LongWritable repostTopAvg;

    public UserBlogInfoDTO() {
    }

    public UserBlogInfoDTO(Text uid) {
        this.uid = uid;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        uid.write(dataOutput);
        releaseFrequency.write(dataOutput);
        attitudeAvg.write(dataOutput);
        commentAvg.write(dataOutput);
        repostAvg.write(dataOutput);
        attitudeMedian.write(dataOutput);
        commentMedian.write(dataOutput);
        commentMedian.write(dataOutput);
        repostMedian.write(dataOutput);
        repostRate.write(dataOutput);

        releaseFrequency2.write(dataOutput);
        attitudeAvg2.write(dataOutput);
        commentAvg2.write(dataOutput);
        repostAvg2.write(dataOutput);
        attitudeMedian2.write(dataOutput);
        commentMedian2.write(dataOutput);
        commentMedian2.write(dataOutput);
        repostMedian2.write(dataOutput);
        repostRate2.write(dataOutput);

        attitudeTopAvg.write(dataOutput);
        commentTopAvg.write(dataOutput);
        repostTopAvg.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        uid = new Text();
        uid.readFields(dataInput);
        releaseFrequency = new FloatWritable();
        releaseFrequency.readFields(dataInput);
        attitudeAvg = new LongWritable();
        attitudeAvg.readFields(dataInput);
        commentAvg = new LongWritable();
        commentAvg.readFields(dataInput);
        repostAvg = new LongWritable();
        repostAvg.readFields(dataInput);
        attitudeMedian = new LongWritable();
        attitudeMedian.readFields(dataInput);
        commentMedian = new LongWritable();
        commentMedian.readFields(dataInput);
        repostMedian = new LongWritable();
        repostMedian.readFields(dataInput);
        repostRate = new FloatWritable();
        repostRate.readFields(dataInput);

        releaseFrequency2 = new FloatWritable();
        releaseFrequency2.readFields(dataInput);
        attitudeAvg2 = new LongWritable();
        attitudeAvg2.readFields(dataInput);
        commentAvg2 = new LongWritable();
        commentAvg2.readFields(dataInput);
        repostAvg2 = new LongWritable();
        repostAvg2.readFields(dataInput);
        attitudeMedian2 = new LongWritable();
        attitudeMedian2.readFields(dataInput);
        commentMedian2 = new LongWritable();
        commentMedian2.readFields(dataInput);
        repostMedian2 = new LongWritable();
        repostMedian2.readFields(dataInput);
        repostRate2 = new FloatWritable();
        repostRate2.readFields(dataInput);

        attitudeTopAvg = new LongWritable();
        attitudeTopAvg.readFields(dataInput);
        commentTopAvg = new LongWritable();
        commentTopAvg.readFields(dataInput);
        repostTopAvg = new LongWritable();
        repostTopAvg.readFields(dataInput);
    }

    public Text getUid() {
        return uid;
    }

    public void setUid(Text uid) {
        this.uid = uid;
    }

    public FloatWritable getReleaseFrequency() {
        return releaseFrequency;
    }

    public void setReleaseFrequency(FloatWritable releaseFrequency) {
        this.releaseFrequency = releaseFrequency;
    }

    public LongWritable getAttitudeAvg() {
        return attitudeAvg;
    }

    public void setAttitudeAvg(LongWritable attitudeAvg) {
        this.attitudeAvg = attitudeAvg;
    }

    public LongWritable getCommentAvg() {
        return commentAvg;
    }

    public void setCommentAvg(LongWritable commentAvg) {
        this.commentAvg = commentAvg;
    }

    public LongWritable getRepostAvg() {
        return repostAvg;
    }

    public void setRepostAvg(LongWritable repostAvg) {
        this.repostAvg = repostAvg;
    }

    public LongWritable getAttitudeMedian() {
        return attitudeMedian;
    }

    public void setAttitudeMedian(LongWritable attitudeMedian) {
        this.attitudeMedian = attitudeMedian;
    }

    public LongWritable getCommentMedian() {
        return commentMedian;
    }

    public void setCommentMedian(LongWritable commentMedian) {
        this.commentMedian = commentMedian;
    }

    public LongWritable getRepostMedian() {
        return repostMedian;
    }

    public void setRepostMedian(LongWritable repostMedian) {
        this.repostMedian = repostMedian;
    }

    public FloatWritable getRepostRate() {
        return repostRate;
    }

    public void setRepostRate(FloatWritable repostRate) {
        this.repostRate = repostRate;
    }

    public FloatWritable getReleaseFrequency2() {
        return releaseFrequency2;
    }

    public void setReleaseFrequency2(FloatWritable releaseFrequency2) {
        this.releaseFrequency2 = releaseFrequency2;
    }

    public LongWritable getAttitudeAvg2() {
        return attitudeAvg2;
    }

    public void setAttitudeAvg2(LongWritable attitudeAvg2) {
        this.attitudeAvg2 = attitudeAvg2;
    }

    public LongWritable getCommentAvg2() {
        return commentAvg2;
    }

    public void setCommentAvg2(LongWritable commentAvg2) {
        this.commentAvg2 = commentAvg2;
    }

    public LongWritable getRepostAvg2() {
        return repostAvg2;
    }

    public void setRepostAvg2(LongWritable repostAvg2) {
        this.repostAvg2 = repostAvg2;
    }

    public LongWritable getAttitudeMedian2() {
        return attitudeMedian2;
    }

    public void setAttitudeMedian2(LongWritable attitudeMedian2) {
        this.attitudeMedian2 = attitudeMedian2;
    }

    public LongWritable getCommentMedian2() {
        return commentMedian2;
    }

    public void setCommentMedian2(LongWritable commentMedian2) {
        this.commentMedian2 = commentMedian2;
    }

    public LongWritable getRepostMedian2() {
        return repostMedian2;
    }

    public void setRepostMedian2(LongWritable repostMedian2) {
        this.repostMedian2 = repostMedian2;
    }

    public FloatWritable getRepostRate2() {
        return repostRate2;
    }

    public void setRepostRate2(FloatWritable repostRate2) {
        this.repostRate2 = repostRate2;
    }

    public LongWritable getAttitudeTopAvg() {
        return attitudeTopAvg;
    }

    public void setAttitudeTopAvg(LongWritable attitudeTopAvg) {
        this.attitudeTopAvg = attitudeTopAvg;
    }

    public LongWritable getCommentTopAvg() {
        return commentTopAvg;
    }

    public void setCommentTopAvg(LongWritable commentTopAvg) {
        this.commentTopAvg = commentTopAvg;
    }

    public LongWritable getRepostTopAvg() {
        return repostTopAvg;
    }

    public void setRepostTopAvg(LongWritable repostTopAvg) {
        this.repostTopAvg = repostTopAvg;
    }

    @Override
    public String toString() {
        return uid + "," +
            releaseFrequency + "," +
            attitudeAvg + "," +
            commentAvg + "," +
            repostAvg + "," +
            attitudeMedian + "," +
            commentMedian + "," +
            repostMedian + "," +
            repostRate + "," +
            releaseFrequency2 + "," +
            attitudeAvg2 + "," +
            commentAvg2 + "," +
            repostAvg2 + "," +
            attitudeMedian2 + "," +
            commentMedian2 + "," +
            repostMedian2 + "," +
            repostRate2 + "," +
            attitudeTopAvg + "," +
            commentTopAvg + "," +
            repostTopAvg;
    }

}
