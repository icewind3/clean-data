package com.cl.data.hbase.service;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/10/25
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class HbaseCommonServiceTest {

    @Resource
    private HbaseCommonService hbaseCommonService;

    @Test
    public void deleteOne() {
        hbaseCommonService.deleteOne("1000111122", 20110062548530535L);
    }

    @Test
    public void deleteRow() {
        hbaseCommonService.deleteRow("161137", 20110061442439425L);
    }
}