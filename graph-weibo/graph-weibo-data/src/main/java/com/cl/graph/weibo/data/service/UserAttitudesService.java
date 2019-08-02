package com.cl.graph.weibo.data.service;

import com.cl.graph.weibo.core.constant.CommonConstants;
import com.cl.graph.weibo.core.constant.FileSuffixConstants;
import com.cl.graph.weibo.data.dto.UserRelationDTO;
import com.cl.graph.weibo.data.entity.UserAttitudes;
import com.cl.graph.weibo.data.entity.UserInfo;
import com.cl.graph.weibo.data.manager.CsvFile;
import com.cl.graph.weibo.data.manager.CsvFileManager;
import com.cl.graph.weibo.data.manager.RedisDataManager;
import com.cl.graph.weibo.data.mapper.marketing2.UserAttitudesMapper;
import com.cl.graph.weibo.data.util.CsvFileHelper;
import com.cl.graph.weibo.data.util.DateTimeUtils;
import com.cl.graph.weibo.data.util.UserInfoUtils;
import com.cl.graph.weibo.data.util.UserTypeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * @author yejianyu
 * @date 2019/7/16
 */
@Slf4j
@Service
public class UserAttitudesService {

    @Resource(name = "commonThreadPoolTaskExecutor")
    private ThreadPoolTaskExecutor taskExecutor;

    @Resource
    private UserAttitudesMapper userAttitudesMapper;

    private final CsvFileManager csvFileManager;
    private final RedisDataManager redisDataManager;

    private static final String USER_ATTITUDES_FILE_DIC = "attitudes";
    private static final String RELATION_ATTITUDES = "_attitudes_";
    private static final String[] HEADER_ATTITUDES = {"from", "to", "mid", "createTime"};

    @Value(value = "${isUserTypeFromFile}")
    private boolean isUserTypeFromFile;

    public UserAttitudesService(CsvFileManager csvFileManager, RedisDataManager redisDataManager) {
        this.csvFileManager = csvFileManager;
        this.redisDataManager = redisDataManager;
    }

    public void genAttitudesFile(String resultPath, String startDate, String endDate) {

        if (isUserTypeFromFile) {
            UserTypeUtils.initUserSet();
        }

        List<String> dateList = DateTimeUtils.getDateList(startDate, endDate);
        CountDownLatch countDownLatch = new CountDownLatch(dateList.size());
        for (String date : dateList) {
            taskExecutor.execute(() -> {
                try {
                    genAttitudesFile(resultPath, date);
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
    }

    public int genAttitudesFile(String resultPath, String tableSuffix) {

        if (isUserTypeFromFile) {
            UserTypeUtils.initUserSet();
        }

        int pageSize = 100000;
        long maxCount;
        long startTime = System.currentTimeMillis();
        log.info("开始处理user_attitudes_{}", tableSuffix);
        try {
            maxCount = userAttitudesMapper.count(tableSuffix);
        } catch (InvalidDataAccessResourceUsageException e) {
            log.warn("表user_attitudes_{}不存在", tableSuffix);
            return 0;
        }
        log.info("统计user_attitudes_{}的总数为{}, 耗时{}ms", tableSuffix, maxCount,
                System.currentTimeMillis() - startTime);
        int count = 0;
        int pageNum = 1;
        Map<String, List<UserRelationDTO>> map = new HashMap<>(6);
        while (count < maxCount) {
            List<UserAttitudes> attitudesList = userAttitudesMapper.findAll(tableSuffix, pageNum, pageSize);
            if (isUserTypeFromFile) {
                wrapDataToMap2(attitudesList, map);
            } else {
                wrapDataToMap(attitudesList, map);
            }

            count += attitudesList.size();
            pageNum++;
            log.info("user_attitudes_{}已处理:{}/{}, 已耗时{}ms", tableSuffix, count, maxCount,
                    System.currentTimeMillis() - startTime);
        }
        File fileDic = new File(resultPath + File.separator + USER_ATTITUDES_FILE_DIC);
        fileDic.mkdir();
        final int[] resultCount = {0};
        map.forEach((key, relationList) -> {
            resultCount[0] += relationList.size();
            String attitudesFileDicName = fileDic.getPath() + File.separator + key;
            File attitudesFileDic = new File(attitudesFileDicName);
            attitudesFileDic.mkdir();
            String filePath = attitudesFileDicName + File.separator + key + CommonConstants.UNDERLINE + tableSuffix
                    + FileSuffixConstants.CSV;
            try {
                writeToCsvFile(relationList, filePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        log.info("user_attitudes_{}已处理完成，实际生成{}条边数据, 共耗时{}ms", tableSuffix, resultCount[0],
                System.currentTimeMillis() - startTime);
        return resultCount[0];
    }

    /**
     * @param dataPath   comment文件夹所在目录
     * @param resultPath 合并结果生成目录
     */
    public void mergeFiles(String dataPath, String resultPath) {
        File attitudesFile = new File(dataPath + File.separator + USER_ATTITUDES_FILE_DIC);
        if (!attitudesFile.exists()) {
            return;
        }
        String[] fileList = attitudesFile.list();
        if (fileList == null || fileList.length == 0) {
            return;
        }
        CountDownLatch mergeCountDownLatch = new CountDownLatch(fileList.length);
        for (String fileName : fileList) {
            taskExecutor.execute(() -> {
                try {
                    mergeFile(attitudesFile.getPath() + File.separator + fileName, resultPath, fileName);
                } finally {
                    mergeCountDownLatch.countDown();
                }
            });
        }
        try {
            mergeCountDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void wrapDataToMap(List<UserAttitudes> comments, Map<String, List<UserRelationDTO>> map) {
        comments.forEach(userAttitudes -> {

            String fromUid = userAttitudes.getCurUid();
            UserInfo fromUserInfo = redisDataManager.getUserInfoViaCache(fromUid);
            if (fromUserInfo == null || !fromUserInfo.isCoreUser()) {
                return;
            }
            String mid = userAttitudes.getMid();
            String toUid = userAttitudes.getMblogUid();
            if (StringUtils.isBlank(toUid)) {
                return;
            }
            UserInfo toUserInfo = redisDataManager.getUserInfoViaCache(toUid);
            if (toUserInfo == null || !toUserInfo.isCoreUser()) {
                return;
            }
            String fromUserType = UserInfoUtils.getUserType(fromUserInfo);
            String toUserType = UserInfoUtils.getUserType(toUserInfo);

            if (StringUtils.isNotBlank(fromUserType) && StringUtils.isNotBlank(toUserType)) {
                UserRelationDTO relationDTO = new UserRelationDTO(fromUid, toUid);
                relationDTO.setMid(mid);
                String createTime = userAttitudes.getTitle();
                relationDTO.setCreateTime(StringUtils.replace(createTime, "/" ,"-"));
                String relationship = fromUserType + RELATION_ATTITUDES + toUserType;
                addToMap(map, relationship, relationDTO);
            }
        });
    }

    private void wrapDataToMap2(List<UserAttitudes> attitudes, Map<String, List<UserRelationDTO>> map) {
        attitudes.forEach(userAttitudes -> {

            String fromUid = userAttitudes.getCurUid();
            Long formUidLong = Long.valueOf(fromUid);
            if (!UserTypeUtils.isCoreUser(formUidLong)) {
                return;
            }

            String mid = userAttitudes.getMid();
            String toUid = userAttitudes.getMblogUid();

            if (StringUtils.isBlank(toUid)) {
                return;
            }
            Long toUidLong = Long.valueOf(toUid);

            if (!UserTypeUtils.isCoreUser(toUidLong)) {
                return;
            }

            String fromUserType = UserTypeUtils.getUserType(formUidLong);
            String toUserType = UserTypeUtils.getUserType(toUidLong);
            if (StringUtils.isNotBlank(fromUserType) && StringUtils.isNotBlank(toUserType)) {
                UserRelationDTO relationDTO = new UserRelationDTO(fromUid, toUid);
                relationDTO.setMid(mid);
                String createTime = userAttitudes.getTitle();
                relationDTO.setCreateTime(StringUtils.replace(createTime, "/" ,"-"));
                String relationship = fromUserType + RELATION_ATTITUDES + toUserType;
                addToMap(map, relationship, relationDTO);
            }
        });
    }

    private void addToMap(Map<String, List<UserRelationDTO>> map, String key, UserRelationDTO relation) {
        if (map.containsKey(key)) {
            map.get(key).add(relation);
        } else {
            List<UserRelationDTO> relationDTOList = new ArrayList<>();
            relationDTOList.add(relation);
            map.put(key, relationDTOList);
        }
    }

    private void writeToCsvFile(List<UserRelationDTO> content, String filePath) throws IOException {
        try (CSVPrinter printer = CsvFileHelper.writer(filePath, HEADER_ATTITUDES)) {
            for (UserRelationDTO userRelation : content) {
                printer.printRecord(userRelation.getFromUid(), userRelation.getToUid(), userRelation.getMid(),
                        userRelation.getCreateTime());
            }
        }
    }

    private void mergeFile(String oriFileDir, String resultDirPath, String resultFileName) {
        File file = new File(oriFileDir);
        if (file.isDirectory()) {
            String[] fileNames = file.list();
            if (fileNames == null || fileNames.length == 0) {
                return;
            }
            List<String> filePathList = new ArrayList<>();
            for (String fileName : fileNames) {
                filePathList.add(oriFileDir + File.separator + fileName);
            }
            CsvFile csvFile = CsvFile.build(resultDirPath, resultFileName).withHeader(HEADER_ATTITUDES);
            csvFileManager.mergeFile(filePathList, csvFile);
        }
    }
}
