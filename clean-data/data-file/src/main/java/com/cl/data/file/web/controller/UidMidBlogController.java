package com.cl.data.file.web.controller;

import com.cl.data.file.service.UidMidBlogFileService;
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
@RequestMapping(value = "/blog")
public class UidMidBlogController {

    @Resource
    private UidMidBlogFileService uidMidBlogFileService;

    @RequestMapping(value = "/genUidMidBlogFile")
    public ResponseResult genUidMidBlogFile(@RequestParam String filePath, @RequestParam String resultPath){
        uidMidBlogFileService.cleanToUidMidBlogFile(filePath, resultPath);
        return ResponseResult.SUCCESS;
    }

    @RequestMapping(value = "/genMblogFromUidFile")
    public ResponseResult genMblogFromUidFile(@RequestParam String filePath, @RequestParam String resultPath){
        uidMidBlogFileService.genMblogFromUidFile(filePath, resultPath);
        return ResponseResult.SUCCESS;
    }

    @RequestMapping(value = "/segmentUidMidBlogFile")
    public ResponseResult segmentUidMidBlogFile(@RequestParam String filePath, @RequestParam String resultPath,
                                                @RequestParam int index){
        uidMidBlogFileService.segmentUidMidBlogFile(filePath, resultPath, index);
        return ResponseResult.SUCCESS;
    }

    @RequestMapping(value = "/segmentUidMidBlogFile2")
    public ResponseResult segmentUidMidBlogFile2(@RequestParam String filePath, @RequestParam String resultPath,
                                                @RequestParam int index){
        uidMidBlogFileService.segmentUidMidBlogFile2(filePath, resultPath, index);
        return ResponseResult.SUCCESS;
    }
}

