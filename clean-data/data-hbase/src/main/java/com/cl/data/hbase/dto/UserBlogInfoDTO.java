package com.cl.data.hbase.dto;

import lombok.Data;

import java.sql.Timestamp;

/**
 * @author yejianyu
 * @date 2019/8/9
 */
@Data
public class UserBlogInfoDTO {

    private Long uid;
    private Integer mblogTotal;
    private Long attitudeSum;
    private Long commentSum;
    private Long repostSum;
    private Timestamp releaseMblogEarly;
    private Timestamp releaseMblogLately;

    public UserBlogInfoDTO(Long uid) {
        this.uid = uid;
    }
}
