package com.cl.graph.weibo.data.web.controller;

import com.cl.graph.weibo.data.entity.MidUidText;
import com.cl.graph.weibo.data.service.MidUidTextService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author yejianyu
 * @date 2019/8/2
 */
@Slf4j
@RequestMapping(value = "/midUid")
@RestController
public class MidUidTextController {

    @Resource(name = "commonThreadPoolTaskExecutor")
    private ThreadPoolTaskExecutor taskExecutor;

    private final MidUidTextService midUidTextService;

    private static final String CREATE_TIME = "2019-08-02";

    public MidUidTextController(MidUidTextService midUidTextService) {
        this.midUidTextService = midUidTextService;
    }

    @RequestMapping(value = "/init")
    public String initMidUid(@RequestParam(name = "size") Long size,
                             @RequestParam(required = false, defaultValue = "10") Integer threadSize) {
        log.info("开始初始化{}条数据", size);
        long startTimeTotal = System.currentTimeMillis();
        ExecutorService executorService = Executors.newFixedThreadPool(threadSize);
        long onsSize = size / threadSize;
        CountDownLatch countDownLatch = new CountDownLatch(threadSize);
        for (int i = 0; i < threadSize; i++) {
            executorService.execute(() -> {
                try {
                    long startTime = System.currentTimeMillis();
                    log.info("开始准备{}条数据", onsSize);
//                    List<MidUidText> list = new ArrayList<>();、
                    int count = 0;
                    for (long j = 0; j < onsSize; j++) {
                        MidUidText midUidText = mockMidUidText();
//                        list.add(midUidText);
                        midUidTextService.insert(midUidText);
                        count++;
                        if (count % 10000 == 0) {
                            log.info("已插入{}条数据， 耗时{}ms", count, System.currentTimeMillis() - startTime);
                        }
                    }
//                    long secondTime = System.currentTimeMillis();
//                    log.info("{}条数据准备完成, 耗时{}ms", onsSize , secondTime - startTime);
//                    final int[] count = {0};
//                    list.forEach(midUidText -> {
//                        midUidTextService.insert(midUidText);
//                        count[0]++;
//                        if (count[0] % 10000 == 0){
//                            log.info("已插入{}条数据， 耗时{}ms", count[0], System.currentTimeMillis() - secondTime);
//                        }
//                    });
                    log.info("插入{}条数据完成， 共耗时{}ms", onsSize, System.currentTimeMillis() - startTime);
                } finally {
                    countDownLatch.countDown();
                }
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        log.info("初始化{}条数据完成,共耗时{}ms", size, System.currentTimeMillis() - startTimeTotal);
        executorService.shutdown();
        return "success";
    }

    @RequestMapping(value = "/init2")
    public String initMidUid2(@RequestParam(name = "size") Long size,
                              @RequestParam(required = false, defaultValue = "20") Integer threadSize) {
        log.info("开始初始化{}条数据", size);
        ExecutorService executorService = Executors.newFixedThreadPool(threadSize);
        long startTime = System.currentTimeMillis();
        int count = 0;
        int oneSize = 1000000;
        Map<String, List<MidUidText>> map = new HashMap<>(1000);
        for (long j = 0; j < size; j++) {
            MidUidText midUidText = mockMidUidText();
            Long uid = midUidText.getUid();
            String index = String.valueOf(uid % 1000);
            if (map.containsKey(index)){
                List<MidUidText> midUidTextList = map.get(index);
                midUidTextList.add(midUidText);
            } else {
                List<MidUidText> midUidTextList = new ArrayList<>();
                midUidTextList.add(midUidText);
                map.put(index, midUidTextList);
            }
            midUidTextService.insert(midUidText);
            count++;
            if (count % oneSize == 0) {
                log.info("已生成{}条数据，开始插入", oneSize);
                CountDownLatch countDownLatch = new CountDownLatch(map.size());
                map.forEach((i, midUidTexts) -> {
                    executorService.execute(() -> {
                        try {
                            midUidTextService.insertAll(i, midUidTexts);
                        }finally {
                            countDownLatch.countDown();
                        }
                    });
                });
                try {
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                log.info("已插入{}条数据， 耗时{}ms", count, System.currentTimeMillis() - startTime);
                map.clear();
            }
        }
        log.info("初始化{}条数据完成,共耗时{}ms", size, System.currentTimeMillis() - startTime);
        executorService.shutdown();
        return "success";
    }

    private MidUidText mockMidUidText() {
        long uid = RandomUtils.nextLong(1L, 9999999999L);
        MidUidText midUidText = new MidUidText();
        midUidText.setUid(uid);
        long midLong = RandomUtils.nextLong();
        String mid = String.valueOf(midLong);
        midUidText.setMid(mid);
        midUidText.setCommentsCount(RandomUtils.nextLong(0, 20000));
        midUidText.setAttitudesCount(RandomUtils.nextLong(0, 20000));
        midUidText.setRepostsCount(RandomUtils.nextLong(0, 20000));
        boolean isRetweeted = RandomUtils.nextInt(1, 10) > 7;
        midUidText.setRetweeted(isRetweeted);
        midUidText.setText(genText());
        midUidText.setCreateTime(CREATE_TIME);
        return midUidText;
    }

    private String genText() {
        StringBuilder str = new StringBuilder();
        int length = RandomUtils.nextInt(2, 20);
        for (int i = 0; i < length; i++) {
            char c = (char) (0x4e00 + (int) (Math.random() * (0x9fa5 - 0x4e00 + 1)));
            str.append(c);
        }
        return str.toString();
    }
}
