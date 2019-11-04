package com.cl.data.social.entity;

import lombok.Data;

/**
 * @author yejianyu
 * @date 2019/7/17
 */
@Data
public class MblogFromUid {

    private String mid;
    private String uid;
    private String pid;
    private String retweetedMid;
    private String retweetedUid;
    private String createTime;

    /**
     * 转发数
     */
    private Long repostsCount;
    /**
     * 评论数
     */
    private Long commentsCount;
    /**
     * 点赞数
     */
    private Long attitudesCount;

    private String text;
    private String retweetedText;
}
