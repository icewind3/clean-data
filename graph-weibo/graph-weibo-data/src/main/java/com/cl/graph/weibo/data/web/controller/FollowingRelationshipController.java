package com.cl.graph.weibo.data.web.controller;

import com.cl.graph.weibo.data.service.FollowingRelationshipService;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author yejianyu
 * @date 2019/7/17
 */
@RequestMapping(value = "/follow")
@RestController
public class FollowingRelationshipController {

    private final FollowingRelationshipService followingRelationshipService;

    public FollowingRelationshipController(FollowingRelationshipService followingRelationshipService) {
        this.followingRelationshipService = followingRelationshipService;
    }

    @RequestMapping(value = "/init")
    public String initFollowingEdge(@RequestParam(name = "oriFilePath") String oriFilePath,
                                  @RequestParam(name = "resultPath") String resultPath){
        followingRelationshipService.genFollowingRelationshipFile(oriFilePath, resultPath);
        return "success";
    }

    @RequestMapping(value = "/init2")
    public String initFollowingEdge2(@RequestParam(name = "oriFilePath") String oriFilePath,
                                    @RequestParam(name = "resultPath") String resultPath){
        followingRelationshipService.genFollowingRelationshipFile2(oriFilePath, resultPath);
        return "success";
    }

    @RequestMapping(value = "/initRemote")
    public String initFollowingEdgeByRemote(@RequestParam(name = "oriFilePath", required = false) String oriFilePath,
                                    @RequestParam(name = "resultPath") String resultPath){
        followingRelationshipService.genFollowingRelationshipFileFromRemote(oriFilePath, resultPath);
        followingRelationshipService.mergeFollowingFiles(resultPath);
        return "success";
    }

    @RequestMapping(value = "/merge")
    public String mergeFollowingEdge(@RequestParam(name = "resultPath") String resultPath){
        followingRelationshipService.mergeFollowingFiles(resultPath);
        return "success";
    }

}
