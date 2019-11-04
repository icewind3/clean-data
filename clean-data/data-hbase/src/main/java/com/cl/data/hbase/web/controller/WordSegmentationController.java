package com.cl.data.hbase.web.controller;

import com.cl.data.hbase.constant.WordSegmentationConstants;
import com.cl.data.hbase.service.WordSegmentationFileService;
import com.cl.graph.weibo.core.web.ResponseResult;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @author yejianyu
 * @date 2019/9/4
 */
@RequestMapping(value = "/blogAnalysis")
@RestController
public class WordSegmentationController {

    @Resource
    private WordSegmentationFileService wordSegmentationFileService;

    @RequestMapping(value = "/result1")
    public ResponseResult processFileResult1(@RequestParam String filePath){
        wordSegmentationFileService.processFileResult(filePath, WordSegmentationConstants.HEADER_RESULT_1,
                WordSegmentationConstants.FAMILY_RESULT_1);
        return ResponseResult.SUCCESS;
    }

    @RequestMapping(value = "/result2")
    public ResponseResult processFileResult2(@RequestParam String filePath){
        wordSegmentationFileService.processFileResult(filePath, WordSegmentationConstants.HEADER_RESULT_2,
                WordSegmentationConstants.FAMILY_RESULT_2);
        return ResponseResult.SUCCESS;
    }

    @RequestMapping(value = "/result3")
    public ResponseResult processFileResult3(@RequestParam String filePath){
        wordSegmentationFileService.processFileResult3(filePath, WordSegmentationConstants.HEADER_RESULT_3,
                WordSegmentationConstants.FAMILY_RESULT_3);
        return ResponseResult.SUCCESS;
    }

    @RequestMapping(value = "/newKol")
    public ResponseResult processFileResult4(@RequestParam String filePath){
        wordSegmentationFileService.processFileResult(filePath, WordSegmentationConstants.HEADER_RESULT_NEW_KOL,
            WordSegmentationConstants.FAMILY_RESULT_1);
        return ResponseResult.SUCCESS;
    }

    @RequestMapping(value = "/kolFans")
    public ResponseResult processKolFans(@RequestParam String filePath){
        wordSegmentationFileService.processFileResult(filePath, WordSegmentationConstants.HEADER_RESULT_FANS,
            WordSegmentationConstants.FAMILY_RESULT_1);
        return ResponseResult.SUCCESS;
    }
}
