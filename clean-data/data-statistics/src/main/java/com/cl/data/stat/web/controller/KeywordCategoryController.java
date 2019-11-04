package com.cl.data.stat.web.controller;

import com.cl.data.stat.service.KeywordAllStatisticsService;
import com.cl.data.stat.service.KeywordCategoryService;
import com.cl.graph.weibo.core.web.ResponseResult;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @author yejianyu
 * @date 2019/9/9
 */
@RequestMapping(value = "/keywordCategory")
@RestController
public class KeywordCategoryController {

    @Resource
    private KeywordCategoryService keywordCategoryService;

    @RequestMapping(value = "/filterType")
    public ResponseResult filterType(@RequestParam String filePath, @RequestParam String resultPath){
        keywordCategoryService.filterType(filePath, resultPath);
        return ResponseResult.SUCCESS;
    }

    @RequestMapping(value = "/computeCategory")
    public ResponseResult computeCategory(@RequestParam String deleteWordFile, @RequestParam String categoryFile,
                                          @RequestParam String filePath,@RequestParam String resultPath){
        keywordCategoryService.computeCategory(deleteWordFile, categoryFile, filePath, resultPath);
        return ResponseResult.SUCCESS;
    }

}
