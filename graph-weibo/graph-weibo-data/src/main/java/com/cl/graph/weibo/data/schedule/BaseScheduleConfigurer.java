package com.cl.graph.weibo.data.schedule;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.scheduling.support.CronTrigger;

import java.util.concurrent.*;

/**
 * @author yejianyu
 * @date 2019/7/23
 */
@Configuration
@EnableScheduling
public abstract class BaseScheduleConfigurer implements SchedulingConfigurer {

    private String cron;

    @Override
    public void configureTasks(ScheduledTaskRegistrar scheduledTaskRegistrar) {
        scheduledTaskRegistrar.setScheduler(taskScheduler());
        scheduledTaskRegistrar.addTriggerTask(
                //执行定时任务
                () -> {
                    if (isStarted()){
                        processTask();
                    }
                },
                //设置触发器
                triggerContext -> {
                    // 初始化定时任务周期
                    if (StringUtils.isEmpty(cron)) {
                        cron = getCron();
                    }
                    CronTrigger trigger = new CronTrigger(cron);
                    return trigger.nextExecutionTime(triggerContext);
                }
        );
    }

    @Bean(destroyMethod = "shutdown")
    public ExecutorService taskScheduler() {
        ThreadFactory namedThreadFactory = new BasicThreadFactory.Builder().namingPattern("weibo-task-%d").daemon(true).build();
        return new ScheduledThreadPoolExecutor(10, namedThreadFactory);
    }

    protected abstract void processTask();

    protected abstract String getCron();

    protected abstract Boolean isStarted();
}
