package com.cl.graph.weibo.api.web.controller;

import com.cl.graph.weibo.api.service.HotCommentService;
import com.cl.graph.weibo.core.constant.CommonConstants;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;

/**
 * @author yejianyu
 * @date 2019/7/17
 */
@RequestMapping(value = "/comment")
@RestController
public class CommentController {

    @Value("${graph.data.root-path}")
    private String dataRootPath;

    private final HotCommentService hotCommentService;

    public CommentController(HotCommentService hotCommentService) {
        this.hotCommentService = hotCommentService;
    }

    @RequestMapping(value = "/genEdge")
    public String genCommentEdge(@RequestParam(name = "date") String date, @RequestParam(name = "sign") String sign){
        String resultPath = dataRootPath + CommonConstants.FORWARD_SLASH + sign + CommonConstants.FORWARD_SLASH + "comment";
        hotCommentService.genCommentFile(resultPath, date, sign);
        return resultPath;
    }

}
