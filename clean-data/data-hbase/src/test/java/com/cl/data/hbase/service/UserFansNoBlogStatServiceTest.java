package com.cl.data.hbase.service;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/10/14
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class UserFansNoBlogStatServiceTest {

    @Resource
    private UserFansNoBlogStatService userFansNoBlogStatService;

    @Test
    public void computeUserFansNoBlogStat() throws IOException {
        String uidFilePath = "C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\fan_mblog\\500w+\\fans";
        String resultPath = "C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\无博文粉丝统计\\500w+";
        userFansNoBlogStatService.computeUserFansNoBlogStat(uidFilePath, resultPath);
    }

    @Test
    public void computeUserFansNoBlogStat2() throws IOException {
        String uidFilePath = "C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\fan_mblog\\500w+\\fans";
        String resultPath = "C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\无博文粉丝统计\\500w+";
        userFansNoBlogStatService.computeUserFansNoBlogStat(uidFilePath, resultPath);
    }
}