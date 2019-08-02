package com.cl.graph.weibo.data.web.controller;

import com.cl.graph.weibo.data.service.UserAttitudesService;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author yejianyu
 * @date 2019/7/17
 */
@RequestMapping(value = "/attitudes")
@RestController
public class UserAttitudesController {

    private final UserAttitudesService userAttitudesService;

    public UserAttitudesController(UserAttitudesService userAttitudesService) {
        this.userAttitudesService = userAttitudesService;
    }

    @RequestMapping(value = "/init")
    public String initAttitudesEdge(@RequestParam(name = "resultPath") String resultPath,
                                  @RequestParam(name = "startDate") String startDate,
                                  @RequestParam(name = "endDate") String endDate){
        userAttitudesService.genAttitudesFile(resultPath, startDate, endDate);
        userAttitudesService.mergeFiles(resultPath, resultPath + "/result");
        return "success";
    }

    @RequestMapping(value = "/mergeFile")
    public String mergeAttitudesEdge(@RequestParam(name = "dataPath") String dataPath){
        userAttitudesService.mergeFiles(dataPath, dataPath + "/result");
        return "success";
    }
}
