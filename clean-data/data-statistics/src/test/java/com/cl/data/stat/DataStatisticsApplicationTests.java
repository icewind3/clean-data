package com.cl.data.stat;

import com.alibaba.fastjson.JSON;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

//@RunWith(SpringRunner.class)
//@SpringBootTest
public class DataStatisticsApplicationTests {

    @Test
    public void contextLoads() {

     int a = 12;
     int top = (int) (0.1 * a) + 1;
        System.out.println(top);
    }

}
