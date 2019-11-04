package com.cl.data.file.web.controller;

import com.cl.data.file.service.FansAgeStatService;
import com.cl.data.file.service.KolUidMidBlogFileService;
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
@RequestMapping(value = "/fansAgeStat")
public class FansAgeStatController {

    @Resource
    private FansAgeStatService fansAgeStatService;

    @RequestMapping(value = "/computeFansAgeGroupStat")
    public ResponseResult computeFansAgeGroupStat(@RequestParam String filePath, @RequestParam String uidFile,
                                            @RequestParam String resultPath){
        fansAgeStatService.computeFansAgeGroupStat2(uidFile, filePath, resultPath);
        return ResponseResult.SUCCESS;
    }


}

