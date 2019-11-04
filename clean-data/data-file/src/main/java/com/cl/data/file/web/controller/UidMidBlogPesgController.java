package com.cl.data.file.web.controller;

import com.cl.data.file.service.UidMidBlogFileService;
import com.cl.data.file.service.UidMidBlogPesgFileService;
import com.cl.graph.weibo.core.web.ResponseResult;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @author yejianyu
 * @date 2019/9/4
 */
@RestController
@RequestMapping(value = "/pesg")
public class UidMidBlogPesgController {

    @Resource
    private UidMidBlogPesgFileService uidMidBlogPesgFileService;

    @RequestMapping(value = "/filterResult")
    public ResponseResult filterResult(@RequestParam String oriFilePath){
        String uidMidFilePath = "/data6_1/user_blog_stat/uid_kol_pesg/uid_mid_top10.csv";
        String resultPath = "/data6_1/user_blog_stat/uid_kol_pesg/uid_mid_blog_pesg_top10.csv";
        uidMidBlogPesgFileService.filterBlogPesgResult(uidMidFilePath, oriFilePath, resultPath);
        return ResponseResult.SUCCESS;
    }

    @RequestMapping(value = "/filterResult2")
    public ResponseResult filterResult2(@RequestParam String uidMidFilePath, @RequestParam String oriFilePath,
                                        @RequestParam String resultPath){
        uidMidBlogPesgFileService.filterBlogPesgResult(uidMidFilePath, oriFilePath, resultPath);
        return ResponseResult.SUCCESS;
    }

    @RequestMapping(value = "/filterResult3")
    public ResponseResult filterResult3(@RequestParam String uidMidFilePath, @RequestParam String oriFilePath,
                                        @RequestParam String resultPath){
        uidMidBlogPesgFileService.filterBlogPesgResultByType(uidMidFilePath, oriFilePath, resultPath);
        return ResponseResult.SUCCESS;
    }

}

