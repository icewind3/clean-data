package com.cl.graph.weibo.api.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yejianyu
 * @date 2019/8/2
 */
public class TaskStatus {

    public static final Integer TASK_PROCEED = 1;
    public static final Integer TASK_FINISH = 2;
    public static final Integer TASK_NOT_STARTED = 0;


    private static final Map<String, Integer> TASK_STATUS_MAP = new ConcurrentHashMap<>();

    public static Integer getStatus(String key){
        return TASK_STATUS_MAP.getOrDefault(key, TASK_NOT_STARTED);
    }

    public static synchronized boolean addTask(String key){
        if (TASK_STATUS_MAP.containsKey(key)){
            return false;
        }
        TASK_STATUS_MAP.put(key, TASK_PROCEED);
        return true;
    }
}
