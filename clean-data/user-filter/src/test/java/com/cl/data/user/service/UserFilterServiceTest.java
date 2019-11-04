package com.cl.data.user.service;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/9/16
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class UserFilterServiceTest {

    @Autowired
    private UserFilterService userFilterService;

    @Test
    public void filterBlacklist() {
        String filePath = "C:\\Users\\cl32\\Desktop\\user_youhuiquan/user_blog_stat_car_gte50_2.csv";
        String resultPath = "C:\\Users\\cl32\\Desktop\\user_youhuiquan/user_blog_stat_car_match3.csv";
        userFilterService.filterBlacklist(filePath, resultPath);
    }

    @Test
    public void needFilter() {
//        userFilterService.needFilter("1218681123");
        userFilterService.needFilter("1185919070");
    }

    @Test
    public void filterByNameAndIntro() {
        String filePath = "C:\\Users\\cl32\\Documents\\weibo/weibo.csv";
//        String filePath = "C:\\Users\\cl32\\Desktop\\stat\\user_filter_new/finial.csv";
        String resultPath = "C:\\Users\\cl32\\Desktop\\stat\\user_filter_new/user_filter_name3.csv";
        userFilterService.filterByNameAndIntro(filePath, resultPath);
    }


}