package com.cl.graph.weibo.data.web.controller;

import com.cl.graph.weibo.data.service.MblogFromUidService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author yejianyu
 * @date 2019/7/17
 */
@RequestMapping(value = "/blog")
@RestController
public class MblogFromUidController {

    private final MblogFromUidService mblogFromUidService;

    public MblogFromUidController(MblogFromUidService mblogFromUidService) {
        this.mblogFromUidService = mblogFromUidService;
    }

    @RequestMapping(value = "/initRetweet")
    public String initRetweetEdge(@RequestParam(name = "resultPath") String resultPath,
                                  @RequestParam(name = "startDate") String startDate,
                                  @RequestParam(name = "endDate") String endDate) {
        mblogFromUidService.genRetweetFileByDate(resultPath, startDate, endDate);
        mblogFromUidService.mergeRetweetFiles(resultPath, resultPath + "/result");
        return "success";
    }

    @RequestMapping(value = "/initRetweetNoSuffix")
    public String initRetweetNoSuffix(@RequestParam(name = "resultPath") String resultPath) {
        mblogFromUidService.genRetweetFile(resultPath,"");
        return "success";
    }

    @RequestMapping(value = "/initBlog")
    public String initBlog(@RequestParam(name = "startDate") String startDate,
                                  @RequestParam(name = "endDate") String endDate) {
        mblogFromUidService.genAllBlogFileByDate(startDate, endDate);
        return "success";
    }

    @RequestMapping(value = "/initBlogDetail")
    public String initBlogDetail(@RequestParam(name = "startDate") String startDate,
                           @RequestParam(name = "endDate") String endDate) {

        mblogFromUidService.genAllBlogFileByDateDetail(startDate, endDate);
        return "success";
    }

    @RequestMapping(value = "/initBlogDependUid")
    public String initBlogDependUid(@RequestParam(name = "startDate") String startDate,
                           @RequestParam(name = "endDate") String endDate) {
        mblogFromUidService.genBlogFileDepenadUidByDate(startDate, endDate);
        return "success";
    }

    @RequestMapping(value = "/initRetweetDetail")
    public String initRetweetDetailEdge(@RequestParam(name = "resultPath") String resultPath,
                                   @RequestParam(name = "startDate") String startDate,
                                   @RequestParam(name = "endDate") String endDate) {
        mblogFromUidService.genRetweetDetailFileByDate(resultPath, startDate, endDate);
        return "success";
    }

    @RequestMapping(value = "/mergeFile")
    public String mergeFile(@RequestParam(name = "dataPath") String dataPath,
                                 @RequestParam(name = "resultPath", required = false) String resultPath){
        if (StringUtils.isBlank(resultPath)){
            resultPath = dataPath + "/result";
        }
        mblogFromUidService.mergeRetweetFiles(dataPath, resultPath);
        return "success";
    }

    @RequestMapping(value = "/processBlog")
    public String processBlog(@RequestParam(name = "startDate") String startDate,
                                 @RequestParam(name = "endDate") String endDate) {
        mblogFromUidService.insertBlogDepenadUidByDate(startDate, endDate);
        return "success";
    }

    @RequestMapping(value = "/processBlogDetail")
    public String processBlogDetail(@RequestParam(name = "startDate") String startDate,
                              @RequestParam(name = "endDate") String endDate) {
        mblogFromUidService.insertBlogDepenadUidByDate(startDate, endDate);
        return "success";
    }
}
