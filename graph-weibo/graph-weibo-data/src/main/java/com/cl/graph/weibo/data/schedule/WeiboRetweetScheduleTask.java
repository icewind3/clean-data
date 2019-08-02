package com.cl.graph.weibo.data.schedule;

import com.cl.graph.weibo.data.service.MblogFromUidService;
import com.cl.graph.weibo.data.util.DateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.io.File;

/**
 * @author yejianyu
 * @date 2019/7/23
 */
@Slf4j
@Configuration
public class WeiboRetweetScheduleTask extends BaseScheduleConfigurer{

    @Value(value = "${task.weibo-retweet.start}")
    private Boolean started;

    @Value(value = "${task.weibo-retweet.cron}")
    private String cron;

    @Value(value = "${graph.weibo.dataRoot}")
    private String dataRootPath;

    @Value(value = "${graph.weibo.resultRoot}")
    private String resultRootPath;

    @Autowired
    private MblogFromUidService mblogFromUidService;

    @Override
    protected void processTask() {
        log.info("开始执行微博转发数据处理任务");
        String startDate = "20190317";
//        String endDate = DateTimeUtils.yesterday();
        String endDate = "20190620";
        String dataPath = dataRootPath + File.separator + endDate;
        mblogFromUidService.genRetweetFileByDate(dataPath, startDate, endDate);
        mblogFromUidService.mergeRetweetFiles(dataPath, resultRootPath + File.separator + endDate);
        log.info("执行微博转发数据处理任务完成");
    }

    @Override
    protected String getCron() {
        return cron;
    }

    @Override
    protected Boolean isStarted() {
        return started;
    }

}
