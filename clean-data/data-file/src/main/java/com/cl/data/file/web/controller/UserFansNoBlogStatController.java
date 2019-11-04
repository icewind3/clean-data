package com.cl.data.file.web.controller;

import com.cl.data.file.service.UserFansNoBlogStatService;
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
    public ResponseResult fansBlogStat(@RequestParam String hasBlogUidPath, @RequestParam String uidFilePath,
                                       @RequestParam String resultPath) {
        try {
            userFansNoBlogStatService.computeUserFansNoBlogStat(hasBlogUidPath, uidFilePath, resultPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ResponseResult.SUCCESS;
    }

    @RequestMapping(value = "/filterHasBlogUid")
    public ResponseResult filterHasBlogUid(@RequestParam String uidFilePath, @RequestParam String resultPath) {
        try {
            userFansNoBlogStatService.filterHasBlogUid(uidFilePath, resultPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ResponseResult.SUCCESS;
    }

    @RequestMapping(value = "/mergeUid")
    public ResponseResult mergeUid(@RequestParam String uidFilePath, @RequestParam String resultPath) {
        try {
            userFansNoBlogStatService.mergeUid(uidFilePath, resultPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ResponseResult.SUCCESS;
    }

}
