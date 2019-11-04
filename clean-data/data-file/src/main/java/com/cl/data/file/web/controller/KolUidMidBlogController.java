package com.cl.data.file.web.controller;

import com.cl.data.file.service.KolUidMidBlogFileService;
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
@RequestMapping(value = "/kolBlog")
public class KolUidMidBlogController {

    @Resource
    private KolUidMidBlogFileService kolUidMidBlogFileService;

    @RequestMapping(value = "/filterKolBlog")
    public ResponseResult filterKolBlog(@RequestParam String filePath, @RequestParam String uidFile,
                                            @RequestParam String resultPath){
        kolUidMidBlogFileService.filterKolBlog(filePath, uidFile, resultPath);
        return ResponseResult.SUCCESS;
    }

    @RequestMapping(value = "/segmentUidMidBlogFile")
    public ResponseResult segmentUidMidBlogFile(@RequestParam String filePath, @RequestParam String resultPath,
                                                @RequestParam int index){
        kolUidMidBlogFileService.segmentUidMidBlogFile(filePath, resultPath, index);
        return ResponseResult.SUCCESS;
    }

}

