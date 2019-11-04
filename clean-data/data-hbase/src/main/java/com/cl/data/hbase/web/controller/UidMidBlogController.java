package com.cl.data.hbase.web.controller;

import com.cl.data.hbase.service.MblogFromUidService;
import com.cl.data.hbase.service.UidMidBlogFileService;
import com.cl.graph.weibo.core.constant.CommonConstants;
import com.cl.graph.weibo.core.exception.ServiceException;
import com.cl.graph.weibo.core.util.DateTimeUtils;
import com.cl.graph.weibo.core.web.ResponseResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author yejianyu
 * @date 2019/9/4
 */
@Slf4j
@RequestMapping(value = "/mblog")
@RestController
public class UidMidBlogController {

    @Resource
    private UidMidBlogFileService uidMidBlogFileService;

    @Resource
    private MblogFromUidService mblogFromUidService;

    @RequestMapping(value = "/toHbase")
    public ResponseResult toHbase(@RequestParam String filePath){
        uidMidBlogFileService.processFileResult(filePath);
        return ResponseResult.SUCCESS;
    }

    @RequestMapping(value = "/toHbaseFans")
    public ResponseResult toHbaseFans(@RequestParam String filePath){
        uidMidBlogFileService.processFansFileResult(filePath);
        return ResponseResult.SUCCESS;
    }

    @RequestMapping(value = "/bidToHbase")
    public ResponseResult bidToHbase(@RequestParam(name = "startDate") String startDate,
                                     @RequestParam(name = "endDate") String endDate,
                                     @RequestParam(name = "isDetail", defaultValue = "false") Boolean isDetail){

        if (isDetail) {
            processMblogFromUidDetailDataByDate(startDate, endDate, tableName -> {
                try {
                    mblogFromUidService.processBidToHbase(tableName);
                } catch (ServiceException e) {
                    log.error(e.getMessage());
                }
            });
        } else {
            processMblogFromUidDataByDate(startDate, endDate, tableName -> {
                try {
                    mblogFromUidService.processBidToHbase(tableName);
                } catch (ServiceException e) {
                    log.error(e.getMessage());
                }
            });
        }
        return ResponseResult.SUCCESS;
    }

    @RequestMapping(value = "/isRetweetedToHbase")
    public ResponseResult isRetweetedToHbase(@RequestParam(name = "startDate") String startDate,
                                     @RequestParam(name = "endDate") String endDate,
                                     @RequestParam(name = "isDetail", defaultValue = "false") Boolean isDetail){

        if (isDetail) {
            processMblogFromUidDetailDataByDate(startDate, endDate, tableName -> {
                try {
                    mblogFromUidService.processIsRetweetedToHbase(tableName);
                } catch (ServiceException e) {
                    log.error(e.getMessage());
                }
            });
        } else {
            processMblogFromUidDataByDate(startDate, endDate, tableName -> {
                try {
                    mblogFromUidService.processIsRetweetedToHbase(tableName);
                } catch (ServiceException e) {
                    log.error(e.getMessage());
                }
            });
        }
        return ResponseResult.SUCCESS;
    }

    private void processMblogFromUidDataByDate(String startDate, String endDate, Consumer<String> consumer) {
        List<String> dateList = DateTimeUtils.getDateList(startDate, endDate);
        int indexSize = 10;
        for (String date : dateList) {
            for (int i = 0; i < indexSize; i++) {
                String tableName = "mblog_from_uid_" + date + CommonConstants.UNDERLINE + i;
//                String tableName = "mblog_from_uid_" + date;
                consumer.accept(tableName);
            }
        }
    }

    private void processMblogFromUidDetailDataByDate(String startDate, String endDate, Consumer<String> consumer) {
        List<String> dateList = DateTimeUtils.getDateList(startDate, endDate);
        for (String date : dateList) {
            String tableName = "mblog_from_uid_detail_" + date;
//            String tableSuffix = date;
            consumer.accept(tableName);
        }
    }
}
