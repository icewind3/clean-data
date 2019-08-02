package com.cl.graph.weibo.data.web.controller;

import com.cl.graph.weibo.data.service.UserInfoService;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author yejianyu
 * @date 2019/7/18
 */
@RequestMapping(value = "/user")
@RestController
public class UserController {

    private final UserInfoService userInfoService;

    public UserController(UserInfoService userInfoService) {
        this.userInfoService = userInfoService;
    }

    @RequestMapping(value = "/initByTraverse")
    public String initUserByTraverse(@RequestParam(name = "resultPath") String resultPath){
        userInfoService.genUserFileByTraverse(resultPath);
        return "success";
    }

    @RequestMapping(value = "/initOne")
    public String initUser(@RequestParam(name = "tableSuffix") String tableSuffix,
                                     @RequestParam(name = "resultPath") String resultPath){
        userInfoService.genUserInfoFile(resultPath + "/result", tableSuffix);
        return "success";
    }
}
