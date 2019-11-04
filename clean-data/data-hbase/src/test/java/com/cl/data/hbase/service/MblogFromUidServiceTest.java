package com.cl.data.hbase.service;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/10/17
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class MblogFromUidServiceTest {

    @Resource
    private MblogFromUidService mblogFromUidService;

    @Test
    public void processBidToHbase() {
        mblogFromUidService.processBidToHbase("mblog_from_uid_20190326_8");
    }

    @Test
    public void processIsRetweetedToHbaseFromFile() {
        String filePath = "C:\\Users\\cl32\\Desktop\\mblog_from_uid_kol_fans/mblog_from_uid_kol_fan_mblog.csv";
        mblogFromUidService.processIsRetweetedToHbaseFromFile(filePath);
    }
}