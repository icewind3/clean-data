package com.cl.data.hbase.service;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/9/6
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class UidMidBlogFileServiceTest {

    @Resource
    private UidMidBlogFileService uidMidBlogFileService;

    @Test
    public void processFileResult() {
        String filePath = "C:/Users/cl32/Desktop/mblog_from_uid_allbirds/mblog_from_uid_kol_fan_mblog.csv";
        uidMidBlogFileService.processFileResult(filePath);
    }
}