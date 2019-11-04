package com.cl.data.hbase.dto;

import lombok.Data;

import java.sql.Timestamp;

/**
 * @author yejianyu
 * @date 2019/8/9
 */
@Data
public class UserBlogRepostStatDTO {

    private Long uid;
    private Integer blogCount;
    private Integer blogRepostCount;
    private Float repostRatio;

    public UserBlogRepostStatDTO(Long uid) {
        this.uid = uid;
    }
}
