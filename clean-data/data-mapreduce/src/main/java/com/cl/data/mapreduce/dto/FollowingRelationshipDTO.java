package com.cl.data.mapreduce.dto;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author yejianyu
 * @date 2019/7/18
 */
@ToString
@Getter
@Setter
public class FollowingRelationshipDTO {

    @JSONField(name = "master")
    private String to;

    @JSONField(name = "slave")
    private String from;
}
