package com.cl.data.hbase.service;

import com.cl.data.hbase.entity.BlogWeekInfo;
import com.cl.graph.weibo.core.util.CsvFileHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/10/25
 */
//@RunWith(SpringRunner.class)
//@SpringBootTest
@Slf4j
public class UserBciDataStateServiceTest {

    @Resource
    private UserBciDataStateService userBciDataStateService;

    @Test
    public void getBlogWeekInfoList() throws IOException {
        String uidPath = "C:\\Users\\cl32\\Documents\\weibo\\weiboBigGraphNew\\uid_core_new/uid_220w_4.csv";
        String resultPath = "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\bci/user_blog_stat_220w_4.csv";
        String[] header = {"uid","start_date","end_date", "blog_total", "retweet_blog_total", "attitude_sum",
        "comment_sum", "repost_sum", "retweet_blog_attitude_sum", "retweet_blog_comment_sum", "retweet_blog_repost_sum"};
        List<Long> list = new ArrayList<>();
        int count = 0;
        try (CSVParser csvParser = CsvFileHelper.reader(uidPath);
             CSVPrinter csvPrinter = CsvFileHelper.writer(resultPath, header, true)) {
            for (CSVRecord record : csvParser) {
                String uid = record.get(0);
                if ("1691069411".equals(uid)) {
                    continue;
                }
                list.add(Long.parseLong(uid));
                count++;
                if (list.size() >= 500) {
                    try {
                        List<BlogWeekInfo> blogWeekInfoList = userBciDataStateService.getBlogWeekInfoList(list);
                        for (BlogWeekInfo blogWeekInfo : blogWeekInfoList) {
                            csvPrinter.printRecord(blogWeekInfo.getUid(),blogWeekInfo.getStartDate(),
                                blogWeekInfo.getEndDate(), blogWeekInfo.getBlogTotal(), blogWeekInfo.getRetweetBlogTotal(),
                                blogWeekInfo.getAttitudeSum(), blogWeekInfo.getCommentSum(), blogWeekInfo.getRepostSum(),
                                blogWeekInfo.getRepostBlogAttitudeSum(), blogWeekInfo.getRepostBlogCommentSum(),
                                blogWeekInfo.getRepostBlogRepostSum());
                        }
                        csvPrinter.flush();
                        list.clear();
                        log.info("complete {}", count);
                    } catch (Exception e) {
                        csvPrinter.flush();
                        log.error("uid={}", list.get(0), e);
                    }

                }
            }
            if (list.size() > 0) {
                List<BlogWeekInfo> blogWeekInfoList = userBciDataStateService.getBlogWeekInfoList(list);
                for (BlogWeekInfo blogWeekInfo : blogWeekInfoList) {
                    csvPrinter.printRecord(blogWeekInfo.getUid(),blogWeekInfo.getStartDate(),
                        blogWeekInfo.getEndDate(), blogWeekInfo.getBlogTotal(), blogWeekInfo.getRetweetBlogTotal(),
                        blogWeekInfo.getAttitudeSum(), blogWeekInfo.getCommentSum(), blogWeekInfo.getRepostSum(),
                        blogWeekInfo.getRepostBlogAttitudeSum(), blogWeekInfo.getRepostBlogCommentSum(),
                        blogWeekInfo.getRepostBlogRepostSum());
                }
            }
        }
        log.info("success {}", count);
    }

    @Test
    public void test(){
        Timestamp createTime = Timestamp.valueOf("2019-10-27 12:11:00");
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(createTime.getTime());
        calendar.setFirstDayOfWeek(Calendar.MONDAY);

        System.out.println(calendar.get(Calendar.WEEK_OF_YEAR));
        int dayWeek = calendar.get(Calendar.DAY_OF_WEEK);
        if (dayWeek == 1) {
            dayWeek = 7;
        } else {
            dayWeek -= 1;
        }
        // 计算本周开始的时间
        calendar.add(Calendar.DAY_OF_MONTH, 1 - dayWeek);
        Date startDate = calendar.getTime();
        calendar.add(Calendar.DAY_OF_MONTH, 6);
        Date endDate = calendar.getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        System.out.println(sdf.format(startDate));
        System.out.println(sdf.format(endDate));

//        LocalDateTime localDateTime = LocalDateTime.ofInstant(createTime.toInstant(),)
//        LocalDateTime todayEnd = LocalDateTime.of(LocalDate.now(), LocalTime.MAX);

    }

    @Test
    public void test2(){
        Timestamp createTime = Timestamp.valueOf("2019-10-27 20:11:00");
        LocalDate localDate = createTime.toInstant().atZone(ZoneOffset.ofHours(8)).toLocalDate();
        LocalDate startDate = localDate.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY));
        LocalDate endDate = localDate.with(TemporalAdjusters.nextOrSame(DayOfWeek.SUNDAY));
//        LocalDate endDate = startDate.plusDays(6);
//        LocalDateTime todayEnd = LocalDateTime.of(LocalDate.now(), LocalTime.MAX);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        System.out.println(formatter.format(startDate));
        System.out.println(formatter.format(endDate));
        int i = endDate.get(ChronoField.ALIGNED_WEEK_OF_YEAR);
        System.out.println(i);

    }
}