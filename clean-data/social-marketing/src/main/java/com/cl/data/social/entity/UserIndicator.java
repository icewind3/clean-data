package com.cl.data.social.entity;

import lombok.Data;

/**
 * @author yejianyu
 * @date 2019/10/25
 */
@Data
public class UserIndicator {

    Long uid;
    Float bci;
    Float zombieRatio;
    Float waterArmyRatio;
    Float repostRatio;
    String startDate;
    String endDate;
}
