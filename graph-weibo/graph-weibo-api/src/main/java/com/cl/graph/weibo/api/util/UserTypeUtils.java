package com.cl.graph.weibo.api.util;

import com.cl.graph.weibo.core.exception.ServiceException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author yejianyu
 * @date 2019/7/25
 */
@Slf4j
public class UserTypeUtils {

    private static final Set<Long> PERSONAL_CORE_SET = new HashSet<>();
    private static final Set<Long> BLUE_V_SET = new HashSet<>();
    private static final Set<Long> YELLOW_V_SET = new HashSet<>();
    private static final Set<Long> RED_V_SET = new HashSet<>();
    private static final Set<Long> SHOW_V_SET = new HashSet<>();
//    private static final String UID_FILE_PATH = "/data6/weiboGraph/uid";
    private static final String UID_FILE_PATH = "C:/Users/cl32/Documents/weibo/weiboBigGraph/uid";

    public static synchronized void initUserSet() throws ServiceException {
        log.info("开始初始化userSet");

        try {
            initSet(BLUE_V_SET, "uid_blue_v.csv");
            initSet(YELLOW_V_SET, "uid_yellow_v.csv");
            initSet(RED_V_SET, "uid_red_v.csv");
            initSet(SHOW_V_SET, "uid_show_v.csv");
            initSet(PERSONAL_CORE_SET, "uid_personal_core.csv");
        } catch (IOException e) {
            log.error("初始化userSet出错", e);
            throw new ServiceException("初始化userSet出错");
        }
        log.info("初始化userSet完成");
    }

    private static void initSet(Set<Long> set, String fileName) throws IOException {
        if (set.size() != 0){
            return;
        }
        try (CSVParser csvParser = CsvFileHelper.reader(UID_FILE_PATH + File.separator + fileName)){
            for (CSVRecord record : csvParser){
                set.add(Long.valueOf(record.get(0)));
            }
        }
        log.info("{}加载完成, 共生成{}个点", fileName, set.size());
    }

    public static String getUserType(Long uid) {
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

    public static boolean isCoreUser(Long uid) {
        if (uid == null) {
            return false;
        }
        return BLUE_V_SET.contains(uid) || PERSONAL_CORE_SET.contains(uid);
    }

}
