package com.cl.data.social.service;

import com.cl.data.social.entity.UserIndicator;
import com.cl.data.social.mapper.weibo.UserIndicatorMapper;
import com.sun.javafx.scene.control.skin.ListCellSkin;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/10/25
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class UserIndicatorServiceTest {

    @Resource
    private UserIndicatorService userIndicatorService;

    @Resource
    private UserIndicatorMapper userIndicatorMapper;

    @Test
    public void computeZombieRatio() {
        userIndicatorService.computeZombieRatio();
    }
    @Test
    public void insertRepostRatio() {
        String filePath = "C:\\Users\\cl32\\Desktop\\水军/uid_repost_ratio_80w.csv";
        userIndicatorService.insertRepostRatio(filePath);
    }

    @Test
    public void insertBci() {
        String filePath = "C:\\Users\\cl32\\Documents\\weibo\\统计结果\\bci/user_bci_stat_some_20190902_20191027.csv";
        userIndicatorService.insertBci(filePath);
    }
}