package com.cl.data.hbase.service;

import com.cl.data.hbase.constant.WordSegmentationConstants;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/9/4
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class WordSegmentationFileServiceTest {

    @Resource
    private WordSegmentationFileService wordSegmentationFileService;

    @Test
    public void processFile() {
        String filePath = "C:/Users/cl32/Downloads/pesg/mblog_from_uid_3_result.csv";
        wordSegmentationFileService.processFileResult(filePath, WordSegmentationConstants.HEADER_RESULT_1,
                WordSegmentationConstants.FAMILY_RESULT_1);
    }
}