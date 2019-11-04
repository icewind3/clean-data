package com.cl.data.stat.service;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/9/11
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class UserKeywordStatisticsServiceTest {

    @Resource
    private UserKeywordStatisticsService userKeywordStatisticsService;

    @Test
    public void computeUserKeywordStat() {
        String filePath = "C:/Users/cl32/Desktop/stat/uid_mid_blog_30_result.csv";
        String resultPath = "C:/Users/cl32/Desktop/stat/uid_mid_blog_stat.csv";
        userKeywordStatisticsService.computeUserKeywordStat(filePath, resultPath);
    }
    @Test
    public void getUserKeywordStatTop() {
        String filePath = "C:/Users/cl32/Desktop/stat/uid_mid_blog_stat.csv";
        String resultPath = "C:/Users/cl32/Desktop/stat/uid_mid_blog_top.csv";
        userKeywordStatisticsService.getUserKeywordStatTop(filePath, resultPath);
    }
}