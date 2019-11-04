package com.cl.data.user.util;

import org.apache.commons.lang3.StringUtils;

import java.util.regex.Pattern;

/**
 * @author yejianyu
 * @date 2019/9/16
 */
public class UserInfoUtils {

    private final static String NAME_REGEX = ".*[搜找淘扒挖券送发]券.*|.*优惠券.*|.*秒杀.*|.*种草.*|.*剁手.*" +
        "|.*省钱.*|.*白菜价.*|.*薅羊毛.*|.*值得买.*|.*白菜券.*|.*折扣券.*|.*优惠折扣.*|.*购物券.*";

    private final static String NAME_REGEX2 = ".*购物.*|.*持家.*|.*穿搭.*|.*安利.*|.*白菜.*";
    private final static String DESC_REGEX = ".*优惠劵.*|.*领券.*|.*购物.*|.*领取.*|.*免费.*|.*惠搭.*|.*搭配.*|.*淘宝.*" +
        "|.*分享.*|.*优惠.*|.*剁手党.*|.*买买买.*|.*搜券.*|.*省钱.*|.*导购.*|.*白菜价.*|.*红包.*|.*网购.*";

    public static boolean isNameMatching(String text){
        if (StringUtils.isBlank(text)){
            return false;
        }
        return Pattern.matches(NAME_REGEX, text);
    }
    public static boolean isNameMatching2(String text){
        if (StringUtils.isBlank(text)){
            return false;
        }
        return Pattern.matches(NAME_REGEX2, text);
    }

    public static boolean isDescMatching(String text){
        if (StringUtils.isBlank(text)){
            return false;
        }
        return Pattern.matches(DESC_REGEX, text);
    }
}
