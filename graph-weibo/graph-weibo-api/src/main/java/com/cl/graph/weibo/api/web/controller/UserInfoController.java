package com.cl.graph.weibo.api.web.controller;

import com.cl.graph.weibo.api.dto.RealmCountDTO;
import com.cl.graph.weibo.api.service.UserInfoService;
import com.cl.graph.weibo.core.web.ResponseResult;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @author yejianyu
 * @date 2019/7/26
 */
@RestController
@RequestMapping(value = "/userInfo")
public class UserInfoController {

    private final UserInfoService userInfoService;

    public UserInfoController(UserInfoService userInfoService) {
        this.userInfoService = userInfoService;
    }

    @RequestMapping(value = "/listProvinceCounts")
    public ResponseResult listProvinceCounts(@RequestParam String date){
        List<RealmCountDTO> realmCountDTOs = userInfoService.listProvinceCounts(date);
        return ResponseResult.success(realmCountDTOs);
    }

    @RequestMapping(value = "/listGenderCounts")
    public ResponseResult listGenderCounts(@RequestParam String date){
        List<RealmCountDTO> realmCountDTOs = userInfoService.listGenderCounts(date);
        return ResponseResult.success(realmCountDTOs);
    }
}
