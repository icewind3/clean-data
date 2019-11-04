package com.cl.data.hbase.service;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

import java.util.List;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/9/10
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class UidMidBlogHbaseServiceTest {

    @Resource
    private UidMidBlogHbaseService uidMidBlogHbaseService;

    @Autowired
    private HbaseTemplate hbaseTemplate;

    @Test
    public void getUserBlogInfoMap() {
    }

    @Test
    public void isUserExist() {
//        String uid = "1074466072";
        String uid = "1203655862";
        boolean userExist = uidMidBlogHbaseService.isUserExist(uid);
        System.out.println(userExist);
    }

}