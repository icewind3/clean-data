package com.cl.data.hbase.dto;

import lombok.Data;

/**
 * @author yejianyu
 * @date 2019/10/18
 */
@Data
public class BlogDTO {

    private String uid;
    private String mid;
    private String text;
    private String retweetedText;

}
