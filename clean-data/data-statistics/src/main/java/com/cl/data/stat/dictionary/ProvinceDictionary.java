package com.cl.data.stat.dictionary;

import java.util.HashMap;
import java.util.Map;

/**
 * @author yejianyu
 * @date 2019/7/26
 */
public final class ProvinceDictionary {

    private static final Map<String, String> PROVINCE_MAP = new HashMap<>();

    static {
        PROVINCE_MAP.put("11", "北京");
        PROVINCE_MAP.put("12", "天津");
        PROVINCE_MAP.put("13", "河北");
        PROVINCE_MAP.put("14", "山西");
        PROVINCE_MAP.put("15", "内蒙古");
        PROVINCE_MAP.put("21", "辽宁");
        PROVINCE_MAP.put("22", "吉林");
        PROVINCE_MAP.put("23", "黑龙江");
        PROVINCE_MAP.put("31", "上海");
        PROVINCE_MAP.put("32", "江苏");
        PROVINCE_MAP.put("33", "浙江");
        PROVINCE_MAP.put("34", "安徽");
        PROVINCE_MAP.put("35", "福建");
        PROVINCE_MAP.put("36", "江西");
        PROVINCE_MAP.put("37", "山东");
        PROVINCE_MAP.put("41", "河南");
        PROVINCE_MAP.put("42", "湖北");
        PROVINCE_MAP.put("43", "湖南");
        PROVINCE_MAP.put("44", "广东");
        PROVINCE_MAP.put("45", "广西");
        PROVINCE_MAP.put("46", "海南");
        PROVINCE_MAP.put("50", "重庆");
        PROVINCE_MAP.put("51", "四川");
        PROVINCE_MAP.put("52", "贵州");
        PROVINCE_MAP.put("53", "云南");
        PROVINCE_MAP.put("54", "西藏");
        PROVINCE_MAP.put("61", "陕西");
        PROVINCE_MAP.put("62", "甘肃");
        PROVINCE_MAP.put("63", "青海");
        PROVINCE_MAP.put("64", "宁夏");
        PROVINCE_MAP.put("65", "新疆");
        PROVINCE_MAP.put("71", "台湾");
        PROVINCE_MAP.put("81", "香港");
        PROVINCE_MAP.put("82", "澳门");
        PROVINCE_MAP.put("100", "其他");
        PROVINCE_MAP.put("400", "海外");
    }

    public static String getProvince(String code) {
        return PROVINCE_MAP.getOrDefault(code, code);
    }

    private ProvinceDictionary() {
    }
}
