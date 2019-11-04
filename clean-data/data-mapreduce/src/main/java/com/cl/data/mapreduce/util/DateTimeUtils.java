package com.cl.data.mapreduce.util;

import java.sql.Timestamp;
import java.util.regex.Pattern;

/**
 * @author yejianyu
 * @date 2019/9/28
 */
public class DateTimeUtils {
    private static final String DATE_REGEX = "^\\d{4}-\\d{1,2}-\\d{1,2}$";

    public static Timestamp transDateTimeStringToTimestamp(String dateTime) {
        if (Pattern.matches(DATE_REGEX, dateTime)) {
            dateTime = dateTime + " 00:00:00";
        }
        return Timestamp.valueOf(dateTime);
    }

}
