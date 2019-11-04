package com.cl.data.social.entity;

import lombok.Data;

/**
 * @author yejianyu
 * @date 2019/10/25
 */
@Data
public class UserZombieStatus {

    Long uid;
    Integer total;
    Integer zombieTotal;
    Integer suspectedLevel1;
    Integer suspectedLevel2;
    Integer suspectedLevel3;
}
