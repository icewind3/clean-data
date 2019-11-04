package com.cl.data.stat.web.controller;

import com.cl.data.stat.service.UserInfoStatService;
import com.cl.graph.weibo.core.web.ResponseResult;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @author yejianyu
 * @date 2019/9/6
 */
@RequestMapping(value = "/stat")
@RestController
public class StatisticsController {

    @Resource
    private UserInfoStatService userInfoStatService;

    @RequestMapping(value = "/countUserInfo")
    public ResponseResult countUserInfo(@RequestParam String userInfoFilePath, @RequestParam String resultPath){
        userInfoStatService.countUserInfoStat(userInfoFilePath, resultPath);
        return ResponseResult.SUCCESS;
    }


}
