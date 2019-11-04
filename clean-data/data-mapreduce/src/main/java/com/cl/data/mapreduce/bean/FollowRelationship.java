package com.cl.data.mapreduce.bean;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author yejianyu
 * @date 2019/8/21
 */
public class FollowRelationship implements Writable {

    private Text from;

    private Text to;

    public FollowRelationship(Text from, Text to) {
        this.from = from;
        this.to = to;
    }

    public FollowRelationship() {
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        from.write(dataOutput);
        to.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        from = new Text();
        from.readFields(dataInput);
        to = new Text();
        to.readFields(dataInput);
    }

    public Text getFrom() {
        return from;
    }

    public void setFrom(Text from) {
        this.from = from;
    }

    public Text getTo() {
        return to;
    }

    public void setTo(Text to) {
        this.to = to;
    }

    @Override
    public String toString() {
        return from + "," + to;
    }
}
