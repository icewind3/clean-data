package com.cl.graph.weibo.api.service;

import com.cl.graph.weibo.api.dto.RealmCountDTO;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/7/26
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class UserInfoServiceTest {

    @Autowired
    private UserInfoService userInfoService;

    @Test
    public void listProvinceCount() {
        List<RealmCountDTO> realmCountDTOS = userInfoService.listProvinceCounts("201907");
        System.out.println(realmCountDTOS);
    }

    @Test
    public void listGenderCount() {
        List<RealmCountDTO> realmCountDTOS = userInfoService.listGenderCounts("201907");
        System.out.println(realmCountDTOS);
    }
}