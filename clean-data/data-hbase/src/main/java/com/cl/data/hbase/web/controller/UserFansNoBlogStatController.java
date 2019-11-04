package com.cl.data.hbase.web.controller;

import com.cl.data.hbase.service.UserFansNoBlogStatService;
import com.cl.graph.weibo.core.web.ResponseResult;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.io.IOException;

/**
 * @author yejianyu
 * @date 2019/10/14
 */
@RequestMapping(value = "/kol")
@RestController
public class UserFansNoBlogStatController {

    @Resource
    private UserFansNoBlogStatService userFansNoBlogStatService;

    @RequestMapping(value = "/fansBlogStat")
    public ResponseResult a(@RequestParam String uidFilePath, @RequestParam String resultPath) {
        try {
            userFansNoBlogStatService.computeUserFansNoBlogStat(uidFilePath, resultPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ResponseResult.SUCCESS;
    }

}
