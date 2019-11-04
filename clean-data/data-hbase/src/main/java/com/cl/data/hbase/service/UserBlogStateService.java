package com.cl.data.hbase.service;

import com.cl.data.hbase.dto.UserBlogInfoDTO;
import com.cl.data.hbase.entity.UserBlogState;
import com.cl.data.hbase.mapper.marketing.UidCoreMapper;
import com.cl.data.hbase.mapper.weibo.UserBlogStateMapper;
import com.github.pagehelper.PageHelper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * @author yejianyu
 * @date 2019/9/9
 */
@Slf4j
@Service
public class UserBlogStateService {

    @Resource
    private UidMidBlogHbaseService uidMidBlogHbaseService;
    @Resource
    private UidCoreMapper uidCoreMapper;
    @Resource
    private UserBlogStateMapper userBlogStateMapper;

    @Resource(name = "hbaseThreadPoolTaskExecutor")
    private ThreadPoolTaskExecutor hbaseThreadPoolTaskExecutor;

    private static final int ONE_DAY_MILLIS = 1000 * 60 * 60 * 24;

    public void computeUserBlogState() {
        int pageSize = 100;
        long startTime = System.currentTimeMillis();

        log.info("开始处理新增用户信息");

        List<Long> uidList = uidCoreMapper.listAll();
        int maxCount = uidList.size();
        log.info("统计用户数为{}, 耗时{}ms", maxCount, System.currentTimeMillis() - startTime);
        AtomicInteger count = new AtomicInteger();
        int pageNum = 1;
        int pageNumMax = (int) Math.ceil((double) maxCount / pageSize);
        CountDownLatch countDownLatch = new CountDownLatch(pageNumMax - pageNum + 1);
        while (pageNum <= pageNumMax) {
            int fromIndex = (pageNum - 1) * pageSize;
            int toIndex = Math.min(pageNum * pageSize, maxCount);
            List<Long> subUidList = uidList.subList(fromIndex, toIndex);
            final int page = pageNum;
            hbaseThreadPoolTaskExecutor.execute(() -> {
                try {
                    batchProcessFromUserInfo(subUidList);
                    int i = count.addAndGet(subUidList.size());
                    if (i % 10000 == 0) {
                        log.info("task=处理用户博文统计, progress:{}/{}, time={}ms", i, maxCount,
                            System.currentTimeMillis() - startTime);
                    }
                } catch (Exception e) {
                    log.error("处理UserState出错，pageSize={}, pageNum={}", pageSize, page, e);
                } finally {
                    countDownLatch.countDown();
                }
            });
            pageNum++;
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        log.info("End task=用户博文统计, time={}ms", System.currentTimeMillis() - startTime);
    }

    public void computeAllUserBlogState() {

    }

    private void batchProcessFromUserInfo(List<Long> uidList) {
        Map<Long, UserBlogInfoDTO> userBlogInfoMap = uidMidBlogHbaseService.getUserBlogInfoMap(uidList);
        List<UserBlogState> userBlogStateList = new ArrayList<>();
        uidList.forEach(uid -> {
            UserBlogInfoDTO userBlogInfo = userBlogInfoMap.get(uid);
            UserBlogState userBlogState = processOneUserBlogState(uid, userBlogInfo);
            userBlogStateList.add(userBlogState);
        });
        if (userBlogStateList.size() > 0) {
            userBlogStateMapper.insertAll(userBlogStateList);
        }
    }

    private UserBlogState processOneUserBlogState(Long uid, UserBlogInfoDTO userBlogInfo) {
        Timestamp releaseMblogEarly = null;
        Timestamp releaseMblogLately = null;
        int mblogTotal = 0;
        long attitudeSum = 0;
        long commentSum = 0;
        long repostSum = 0;
        UserBlogState userBlogState = new UserBlogState(uid);

        if (userBlogInfo != null) {
            attitudeSum = userBlogInfo.getAttitudeSum();
            commentSum = userBlogInfo.getCommentSum();
            repostSum = userBlogInfo.getRepostSum();
            releaseMblogEarly = userBlogInfo.getReleaseMblogEarly();
            releaseMblogLately = userBlogInfo.getReleaseMblogLately();
            mblogTotal = userBlogInfo.getMblogTotal();
        }

        userBlogState.setMblogTotal(mblogTotal);
        userBlogState.setAttitudeSum(attitudeSum);
        userBlogState.setCommentSum(commentSum);
        userBlogState.setRepostSum(repostSum);
        userBlogState.setReleaseMblogEarly(releaseMblogEarly);
        userBlogState.setReleaseMblogLately(releaseMblogLately);

        if (mblogTotal > 0){
            userBlogState.setAttitudeAvg(attitudeSum / mblogTotal);
            userBlogState.setCommentAvg(commentSum / mblogTotal);
            userBlogState.setRepostAvg(repostSum / mblogTotal);
        }

        if (releaseMblogLately == null || releaseMblogEarly == null) {
            return userBlogState;
        }

        Timestamp recentTime = releaseMblogLately;

        if (mblogTotal == 1) {
            recentTime = Timestamp.valueOf(LocalDateTime.now());
        }
        long days = (recentTime.getTime() - releaseMblogEarly.getTime()) / (ONE_DAY_MILLIS) + 1;
        float releaseMblogFrequency = (float) mblogTotal / days;
        userBlogState.setReleaseMblogFrequency(releaseMblogFrequency);
        return userBlogState;
    }
}
