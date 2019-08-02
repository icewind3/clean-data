package com.cl.graph.weibo.data.util;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yejianyu
 * @date 2019/7/22
 */
public class DateTimeUtils {

    private static final DateTimeFormatter DEFAULT_DATE_FORMATTER_DEFAULT = DateTimeFormatter.ofPattern("yyyyMMdd");

    public static String today(){
        return DEFAULT_DATE_FORMATTER_DEFAULT.format(LocalDate.now());
    }

    public static String yesterday(){
        LocalDate now = LocalDate.now();
        return DEFAULT_DATE_FORMATTER_DEFAULT.format(now.plusDays(-1L));
    }

    public static List<String> getDateList(String startDate, String endDate) throws DateTimeParseException {
        LocalDate fromDate = LocalDate.from(DEFAULT_DATE_FORMATTER_DEFAULT.parse(startDate));
        LocalDate toDate = LocalDate.from(DEFAULT_DATE_FORMATTER_DEFAULT.parse(endDate));
        List<String> dateList = new ArrayList<>();
        while (!fromDate.isAfter(toDate)) {
            dateList.add(DEFAULT_DATE_FORMATTER_DEFAULT.format(fromDate));
            fromDate = fromDate.plusDays(1L);
        }
        return dateList;
    }
}
