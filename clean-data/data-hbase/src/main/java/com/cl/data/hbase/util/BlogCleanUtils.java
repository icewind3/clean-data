package com.cl.data.hbase.util;

import org.apache.commons.lang3.StringUtils;

/**
 * @author yejianyu
 * @date 2019/8/23
 */
public class BlogCleanUtils {

    private final static String CRLF_REGEX = "[\\n\\r~]";
    private final static String UNLESS_REGEX = "//( )?@.*?[:：]|回复( )?@.*?[:：]|@.*? |^转发微博[。.]$|^转发微博$";

    public static String cleanBlog(String text){
        String s = removeUnlessText(text);
        return replaceCrlf(s);
    }

    public static String replaceCrlf(String text){
        if (StringUtils.isBlank(text)){
            return StringUtils.EMPTY;
        }
        return text.replaceAll(CRLF_REGEX, StringUtils.SPACE);
    }

    public static String removeUnlessText(String text){
        if (StringUtils.isBlank(text)){
            return StringUtils.EMPTY;
        }
        return text.replaceAll(UNLESS_REGEX, StringUtils.EMPTY);
    }
}
