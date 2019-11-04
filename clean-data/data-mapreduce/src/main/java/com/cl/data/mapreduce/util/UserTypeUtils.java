package com.cl.data.mapreduce.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author yejianyu
 * @date 2019/7/25
 */
@Slf4j
public class UserTypeUtils {

    private static final Set<String> PERSONAL_CORE_SET = new HashSet<>();
    private static final Set<String> BLUE_V_SET = new HashSet<>();
    private static final Set<String> YELLOW_V_SET = new HashSet<>();
    private static final Set<String> RED_V_SET = new HashSet<>();
    private static final Set<String> SHOW_V_SET = new HashSet<>();

    public static void initSet(String fileName) {
        switch (fileName) {
            case "uid_blue_v.csv":
                initSet(BLUE_V_SET, fileName);
                break;
            case "uid_yellow_v.csv":
                initSet(YELLOW_V_SET, fileName);
                break;
            case "uid_red_v.csv":
                initSet(RED_V_SET, fileName);
                break;
            case "uid_show_v.csv":
                initSet(SHOW_V_SET, fileName);
                break;
            case "uid_personal_core.csv":
                initSet(PERSONAL_CORE_SET, fileName);
                break;
            default:
                break;
        }
    }

    private static void initSet(Set<String> set, String fileName) {
        log.info("开始加载{}, ", fileName);
        try {
            BufferedReader fis = new BufferedReader(new FileReader(fileName));
            String uid;
            while ((uid = fis.readLine()) != null) {
                set.add(uid);
            }
        } catch (IOException ioe) {
            System.err.println("Caught exception while parsing the cached file '"
                    + org.apache.hadoop.util.StringUtils.stringifyException(ioe));
        }
        log.info("{}加载完成, 共生成{}个点", fileName, set.size());
    }

    public static String getUserType(String uid) {
        if (uid == null) {
            return StringUtils.EMPTY;
        }
        if (SHOW_V_SET.contains(uid)) {
            return "showV";
        } else if (YELLOW_V_SET.contains(uid)) {
            return "yellowV";
        } else if (BLUE_V_SET.contains(uid)) {
            return "blueV";
        } else if (RED_V_SET.contains(uid)) {
            return "redV";
        } else if (PERSONAL_CORE_SET.contains(uid)) {
            return "person";
        } else {
            return "fans";
        }
    }

    public static boolean isCoreUser(String uid) {
        if (uid == null) {
            return false;
        }
        return BLUE_V_SET.contains(uid) || PERSONAL_CORE_SET.contains(uid);
    }

}
