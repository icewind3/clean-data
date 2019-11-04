package com.cl.data.mapreduce.bean;

import lombok.Data;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author yejianyu
 * @date 2019/9/24
 */
@Data
public class TopInput implements WritableComparable<TopInput> {


    private Text type;

    private Text value;

    public TopInput() {
        set(new Text(), new Text());
    }

    public TopInput(Text type, Text value) {
        this.type = type;
        this.value = value;
    }

    public void set(Text type, Text value) {
        this.type = type;
        this.value = value;
    }

    @Override
    public int compareTo(TopInput o) {
        return value.compareTo(o.value);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        type.write(dataOutput);
        value.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        type.readFields(dataInput);
        value.readFields(dataInput);
    }

}
