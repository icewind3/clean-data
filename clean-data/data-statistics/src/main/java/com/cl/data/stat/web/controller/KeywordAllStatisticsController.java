package com.cl.data.stat.web.controller;

import com.cl.data.stat.service.KeywordAllStatisticsService;
import com.cl.graph.weibo.core.web.ResponseResult;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @author yejianyu
 * @date 2019/9/9
 */
@RequestMapping(value = "/keywordStat")
@RestController
public class KeywordAllStatisticsController {

    @Resource
    private KeywordAllStatisticsService keywordAllStatisticsService;

    @RequestMapping(value = "/countByPeopleStat")
    public ResponseResult countByPeopleStat(@RequestParam String filePath, @RequestParam String resultPath){
        keywordAllStatisticsService.countByPeopleStat(filePath, resultPath);
        return ResponseResult.SUCCESS;
    }

    @RequestMapping(value = "/countByWordFrequency")
    public ResponseResult countByWordFrequency(@RequestParam String filePath, @RequestParam String resultPath){
        keywordAllStatisticsService.countByWordFrequency(filePath, resultPath);
        return ResponseResult.SUCCESS;
    }
}
