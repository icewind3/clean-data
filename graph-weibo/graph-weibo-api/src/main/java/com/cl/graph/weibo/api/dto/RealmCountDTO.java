package com.cl.graph.weibo.api.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author yejianyu
 * @date 2019/7/26
 */
@Getter
@Setter
@ToString
public class RealmCountDTO {

    String name;
    Integer count;

    public static RealmCountDTO build(String name, Integer count) {
        return new RealmCountDTO(name, count);
    }

    private RealmCountDTO(String name, Integer count) {
        this.name = name;
        this.count = count;
    }
}
