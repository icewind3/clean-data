package com.cl.graph.weibo.api.web.controller;

import com.cl.graph.weibo.api.service.FollowingRelationshipService;
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
@RequestMapping(value = "/follow")
@RestController
public class FollowingRelationshipController {

    @Value("${graph.data.root-path}")
    private String dataRootPath;

    private final FollowingRelationshipService followingRelationshipService;

    public FollowingRelationshipController(FollowingRelationshipService followingRelationshipService) {
        this.followingRelationshipService = followingRelationshipService;
    }

    @RequestMapping(value = "/processFollow")
    public String processFollow(@RequestParam(name = "filePath") String filePath,
                                  @RequestParam(name = "sign") String sign){
        String resultPath  = dataRootPath + CommonConstants.FORWARD_SLASH + sign + CommonConstants.FORWARD_SLASH + "following";
        followingRelationshipService.genFollowingRelationshipFile(filePath, resultPath, sign);
        return resultPath;
    }


}
