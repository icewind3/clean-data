package com.cl.data.user.util;

import org.apache.commons.lang3.StringUtils;

import java.util.regex.Pattern;

/**
 * @author yejianyu
 * @date 2019/9/16
 */
public class BlogContentUtils {

//    private final static String COUPON_REGEX = ".*【￥?\\d+(.\\d*)?】.*|.*【\\d+(.\\d*)?(起|元券)】.*|.*【领劵拍】.*|.*【赠运费险】.*|.*【售价】.*|.*【购买】.*|.*【第二件\\d+(.\\d*)?元】.*";
    private final static String COUPON_REGEX = ".*【￥?\\d+(.\\d*)?】.*|.*【\\d+(.\\d*)?(起|元券)】.*|.*【领劵拍】.*" +
    "|.*【赠运费险】.*|.*【售价】.*|.*【购买】.*|.*【[第拍][2二]件[仅]?\\d+(.\\d*)?】.*|.*【下单立减\\d+(.\\d*)?元】.*" +
    "|.*【数量有限，抢完为止】.*|.*【买一送二】.*|.*[领找搜]券.*|.*找券.*|.*￥?\\d+(.\\d*)?元券.*|.*请戳评论了解.*" +
    "|.*到手只要.*|.*运费险.*|.*如券失效戳.*|.*[第拍][2二]件[仅]?\\d+(.\\d*)?.*";

    public static boolean isSpam(String text){
        if (StringUtils.isBlank(text)){
            return false;
        }
        return Pattern.matches(COUPON_REGEX, text);
    }
}
