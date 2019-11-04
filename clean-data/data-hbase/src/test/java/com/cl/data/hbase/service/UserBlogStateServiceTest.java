package com.cl.data.hbase.service;

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
public class UserBlogStateServiceTest {

    @Resource
    private UserBlogStateService userBlogStateService;

    @Test
    public void computeUserBlogState() {
        userBlogStateService.computeUserBlogState();
    }
}