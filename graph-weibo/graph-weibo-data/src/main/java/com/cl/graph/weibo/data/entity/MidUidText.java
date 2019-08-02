package com.cl.graph.weibo.data.entity;

import lombok.Data;

/**
 * @author yejianyu
 * @date 2019/7/17
 */
@Data
public class MidUidText {

    private String mid;

    private Long uid;

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

    private Boolean retweeted;

    private String createTime;
}
