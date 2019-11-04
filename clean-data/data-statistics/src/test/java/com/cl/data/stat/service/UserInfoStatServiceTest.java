package com.cl.data.stat.service;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/9/9
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class UserInfoStatServiceTest {

    @Resource
    private UserInfoStatService userInfoStatService;

    @Test
    public void countUserInfoGenderStat() {
        String userInfoFile = "C:\\Users\\cl32\\Documents\\weibo\\统计结果/user_info.csv";
        String resultPath = "C:\\Users\\cl32\\Desktop\\userinfo";
        userInfoStatService.countUserInfoGenderStat(userInfoFile, resultPath);
    }
}