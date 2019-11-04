package com.cl.data.stat.web.controller;

import com.cl.data.stat.service.UserKeywordStatisticsService;
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
public class UserKeywordStatController {

    @Resource
    private UserKeywordStatisticsService userKeywordStatisticsService;

    @RequestMapping(value = "/countUserKeyword")
    public ResponseResult countUserKeyword(@RequestParam String filePath, @RequestParam String resultPath){
        userKeywordStatisticsService.computeUserKeywordStat(filePath, resultPath);
        return ResponseResult.SUCCESS;
    }

    @RequestMapping(value = "/countCoreUserKeyword")
    public ResponseResult countCoreUserKeyword(@RequestParam String filePath, @RequestParam String resultPath){
        userKeywordStatisticsService.computeCoreUserKeywordStat(filePath, resultPath);
        return ResponseResult.SUCCESS;
    }

    @RequestMapping(value = "/getUserKeywordStatTop")
    public ResponseResult getUserKeywordStatTop(@RequestParam String filePath, @RequestParam String resultPath){
        userKeywordStatisticsService.getUserKeywordStatTop(filePath, resultPath);
        return ResponseResult.SUCCESS;
    }


}
