package com.cl.graph.weibo.api.web.controller;

import com.cl.graph.weibo.api.service.MblogFromUidService;
import com.cl.graph.weibo.core.constant.CommonConstants;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;

/**
 * @author yejianyu
 * @date 2019/7/17
 */
@RequestMapping(value = "/blog")
@RestController
public class MblogFromUidController {

    @Value("${graph.data.root-path}")
    private String dataRootPath;

    private final MblogFromUidService mblogFromUidService;

    public MblogFromUidController(MblogFromUidService mblogFromUidService) {
        this.mblogFromUidService = mblogFromUidService;
    }

    @RequestMapping(value = "/processRetweet")
    public String processRetweet(@RequestParam(name = "date") String date, @RequestParam(name = "sign") String sign) {
        String resultPath  = dataRootPath + CommonConstants.FORWARD_SLASH + sign + CommonConstants.FORWARD_SLASH + "retweet";
        mblogFromUidService.genRetweetFile(resultPath, date, sign);
        return resultPath;
    }

}
