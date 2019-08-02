package com.cl.graph.weibo.data.web.controller;

import com.cl.graph.weibo.data.manager.CsvFile;
import com.cl.graph.weibo.data.manager.CsvFileManager;
import com.cl.graph.weibo.data.service.HotCommentService;
import org.apache.commons.lang3.StringUtils;
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

    private final HotCommentService hotCommentService;

    public CommentController(HotCommentService hotCommentService) {
        this.hotCommentService = hotCommentService;
    }

    @RequestMapping(value = "/init")
    public String initCommentEdge(@RequestParam(name = "resultPath") String resultPath){
        String startDate = "20190415";
        String endDate = "20190418";
        hotCommentService.genCommentFile(resultPath, startDate, endDate);
        return "success";
    }

    @RequestMapping(value = "/initOne")
    public String initCommentEdgeFromOneTable(@RequestParam(name = "tableSuffix") String tableSuffix,
                                  @RequestParam(name = "resultPath") String resultPath){
        hotCommentService.genCommentFile(resultPath, tableSuffix);
        hotCommentService.mergeCommentFiles(resultPath, resultPath + "/result");
        return "success";
    }

    @RequestMapping(value = "/mergeFile")
    public String mergeFile(@RequestParam(name = "dataPath") String dataPath,
                            @RequestParam(name = "resultPath", required = false) String resultPath){
        if (StringUtils.isBlank(resultPath)){
            resultPath = dataPath + "/result";
        }
        hotCommentService.mergeCommentFiles(dataPath, resultPath);
        return "success";
    }
}
