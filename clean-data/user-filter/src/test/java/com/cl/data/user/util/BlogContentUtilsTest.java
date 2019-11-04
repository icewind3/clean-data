package com.cl.data.user.util;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.regex.Pattern;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/9/16
 */
public class BlogContentUtilsTest {

    @Test
    public void isSpam() {
//        String text = "aa【领劵拍】潮流单品牛角扣连帽毛呢外套 超级温柔可爱的颜色";
        String text = "aa【19.8】BSM 橡胶记忆乳胶枕";
        boolean spam = BlogContentUtils.isSpam(text);
        System.out.println(spam);
    }

    @Test
    public void isSpam2() {
//        String text = "aa【领劵拍】潮流单品牛角扣连帽毛呢外套 超级温柔可爱的颜色";
        String text = "【第2件仅11.32元】";
        boolean spam = isSpam(text);
        System.out.println(spam);
    }

    private final static String COUPON_REGEX = ".*【[第拍][2二]件[仅]?\\d+(.\\d*)?】.*";

    private boolean isSpam(String text){
        if (StringUtils.isBlank(text)){
            return false;
        }
        return Pattern.matches(COUPON_REGEX, text);
    }
}