package com.cl.data.stat.web.controller;

import com.cl.data.stat.service.KeywordStatisticsService;
import com.cl.graph.weibo.core.web.ResponseResult;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @author yejianyu
 * @date 2019/9/9
 */
@RequestMapping(value = "/stat")
@RestController
public class KeywordStatisticsController {

//    private static final String[] HEADER_ONE = {"uid", "mid", "ebook", "enterprise", "music", "film", "city", "product", "brand", "star",
//        "app", "created_at", "weight"};

    private static final String[] HEADER_ONE = {"uid", "mid", "ebook", "enterprise", "music", "film", "city", "product", "brand", "star",
        "app", "created_at", "weight"};

    @Resource
    private KeywordStatisticsService keywordStatisticsService;

    @RequestMapping(value = "/countByWordFrequency")
    public ResponseResult countByWordFrequency(@RequestParam String filePath, @RequestParam String resultPath){
        keywordStatisticsService.countByWordFrequencyFromOneFile(filePath, resultPath, HEADER_ONE);
        return ResponseResult.SUCCESS;
    }

    @RequestMapping(value = "/countByPeopleStatistic")
    public ResponseResult countByPeopleStatistic(@RequestParam String filePath, @RequestParam String resultPath){
        keywordStatisticsService.countByPeopleStatisticFromOneFile(filePath, resultPath, HEADER_ONE);
        return ResponseResult.SUCCESS;
    }
}
