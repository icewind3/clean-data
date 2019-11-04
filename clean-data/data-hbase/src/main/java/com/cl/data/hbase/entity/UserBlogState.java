package com.cl.data.hbase.entity;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class UserBlogState {

    private Long uid;
    private Integer mblogTotal;
    private Long attitudeSum;
    private Long commentSum;
    private Long repostSum;
    private Long attitudeAvg;
    private Long commentAvg;
    private Long repostAvg;
    private Timestamp releaseMblogEarly;
    private Timestamp releaseMblogLately;
    private Float releaseMblogFrequency;

    public UserBlogState(Long uid){
        this.uid = uid;
    }
}
