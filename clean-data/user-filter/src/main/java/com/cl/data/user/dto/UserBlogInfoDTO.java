package com.cl.data.user.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author yejianyu
 * @date 2019/9/16
 */
@Getter
@Setter
@ToString
public class UserBlogInfoDTO {

    String mid;
    String text;
    String retweetedText;
}
