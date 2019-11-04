package com.cl.data.hbase.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Map;

/**
 * @author yejianyu
 * @date 2019/9/4
 */
@Getter
@Setter
@ToString
public class BlogAnalysisResultDTO {

    private String uid;
    private String mid;
    private Map<String, String> typeWordMap;
    private String createdAt;
}
