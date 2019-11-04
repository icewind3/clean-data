package com.cl.data.mapreduce.bean;

import lombok.Data;

import java.util.Map;

@Data
public class LabelCategory {

    private int column;
    private Map<String, String> category;
}
