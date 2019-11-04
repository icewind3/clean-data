package com.cl.data.hbase.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author yejianyu
 * @date 2019/7/18
 */
@Configuration
public class ThreadPoolConfig {

    @Bean(name = "hbaseThreadPoolTaskExecutor")
    public ThreadPoolTaskExecutor hbaseThreadPoolTaskExecutor() {
        return threadPoolTaskExecutor("hbaseTask-", 20);
    }

    private ThreadPoolTaskExecutor threadPoolTaskExecutor(String threadNamePrefix) {
        return threadPoolTaskExecutor(threadNamePrefix, 10);
    }

    private ThreadPoolTaskExecutor threadPoolTaskExecutor(String threadNamePrefix, int corePoolSize) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(corePoolSize);
        executor.setMaxPoolSize(100);
        executor.setQueueCapacity(500000);
        executor.setKeepAliveSeconds(60);
        executor.setThreadNamePrefix(threadNamePrefix);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        return executor;
    }


}
