package com.cl.graph.weibo.data.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author yejianyu
 * @date 2019/7/17
 */
@ToString
@Getter
@Setter
public class UserRelationDTO {

    private String fromUid;
    private String toUid;
    private String mid;
    private String createTime;

    public UserRelationDTO(String fromUid, String toUid) {
        this.fromUid = fromUid;
        this.toUid = toUid;
    }
}
