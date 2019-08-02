package com.cl.graph.weibo.data.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.cl.graph.weibo.core.constant.CommonConstants;
import com.cl.graph.weibo.core.constant.FileSuffixConstants;
import com.cl.graph.weibo.core.linux.LinuxAction;
import com.cl.graph.weibo.core.linux.LinuxUtil;
import com.cl.graph.weibo.core.linux.ResultApi;
import com.cl.graph.weibo.data.dto.FollowingRelationshipDTO;
import com.cl.graph.weibo.data.entity.UserInfo;
import com.cl.graph.weibo.data.manager.CsvFile;
import com.cl.graph.weibo.data.manager.CsvFileManager;
import com.cl.graph.weibo.data.manager.RedisDataManager;
import com.cl.graph.weibo.data.util.CsvFileHelper;
import com.cl.graph.weibo.data.util.UserInfoUtils;
import com.cl.graph.weibo.data.util.UserTypeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * @author yejianyu
 * @date 2019/7/18
 */
@Slf4j
@Service
public class FollowingRelationshipService {

    private final RedisDataManager redisDataManager;
    private final CsvFileManager csvFileManager;

    @Resource(name = "followingThreadPoolTaskExecutor")
    private ThreadPoolTaskExecutor followingTaskExecutor;

    private static final String FOLLOWING_FILE_DIR = "following";
    private static final String RELATION_FOLLOWING = "_following_";
    private static final String[] HEADER_FOLLOWING = {"from", "to", "createTime"};

    private static final String RESULT_DIR_NAME = "result";

    @Value(value = "${following.server.ip}")
    private String followingServerIp;
    @Value(value = "${following.server.user}")
    private String followingServerUser;
    @Value(value = "${following.server.password}")
    private String followingServerPassword;
    @Value(value = "${following.server.dataRoot}")
    private String defaultFollowingDataDirectory;

    public FollowingRelationshipService(RedisDataManager redisDataManager, CsvFileManager csvFileManager) {
        this.redisDataManager = redisDataManager;
        this.csvFileManager = csvFileManager;
    }

    public void genFollowingRelationshipFile2(String filePath, String resultPath) {
        List<String> filePathList = getAllFilePath(filePath);
        if (filePathList == null || filePathList.size() == 0) {
            return;
        }
        UserTypeUtils.initUserSet();
        if (filePathList.size() == 1) {
            genFollowingRelationshipFileFromOneFile2(filePathList.get(0), resultPath);
        } else {
            filePathList.forEach(file -> {
                genFollowingRelationshipFileFromOneFile2(file, resultPath);
            });
        }
    }

    private void genFollowingRelationshipFileFromOneFile2(String filePath, String resultPath) {
        File file = new File(filePath);
        Map<String, Set<Long>> nonCoreUserMap = new HashMap<>();
        log.info("开始处理文件{}", file.getPath());
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
            int countEdge = 0;
            int infoThresholdStep = 100000;
            int infoThresholdCount = infoThresholdStep;
            String line;
            Map<String, List<FollowingRelationshipDTO>> map = new HashMap<>(16);
            while ((line = br.readLine()) != null) {
                List<FollowingRelationshipDTO> relationshipList;
                try {
                    relationshipList = JSON.parseArray(line, FollowingRelationshipDTO.class);
                } catch (JSONException e) {
                    e.printStackTrace();
                    continue;
                }
                for (FollowingRelationshipDTO followingRelationship : relationshipList) {
                    Long toUid = followingRelationship.getTo();
                    if (!UserTypeUtils.isCoreUser(toUid)) {
                        continue;
                    }

                    Long fromUid = followingRelationship.getFrom();
                    String fromUserType = UserTypeUtils.getUserType(fromUid);
                    if (!UserTypeUtils.isCoreUser(fromUid)){
                        addToNonCoreUserMap(nonCoreUserMap, fromUserType, fromUid);
                    }
                    String toUserType = UserTypeUtils.getUserType(toUid);
                    if (StringUtils.isNotBlank(fromUserType) && StringUtils.isNotBlank(toUserType)) {
                        String relationship = fromUserType + RELATION_FOLLOWING + toUserType;
                        if (map.containsKey(relationship)) {
                            map.get(relationship).add(followingRelationship);
                        } else {
                            List<FollowingRelationshipDTO> followingRelationshipList = new ArrayList<>();
                            followingRelationshipList.add(followingRelationship);
                            map.put(relationship, followingRelationshipList);
                        }
                    }
                }
                File fileDir = new File(resultPath + File.separator + FOLLOWING_FILE_DIR);
                fileDir.mkdir();
                countEdge += writeToFile(map, fileDir.getPath(), file.getParentFile().getName()
                        + CommonConstants.UNDERLINE + file.getName());
                if (countEdge >= infoThresholdCount) {
                    infoThresholdCount = (countEdge / infoThresholdStep + 1) * infoThresholdStep;
                    log.info("处理文件{},已生成{}条边", filePath, countEdge);
                }
                map.clear();
            }

            String nonCoreUser = resultPath + File.separator + "nonCoreUser";
            File nonCoreUserDir = new File(nonCoreUser);
            nonCoreUserDir.mkdirs();
            nonCoreUserMap.forEach((type, uidSet) -> {
                try (CSVPrinter csvPrinter = CsvFileHelper.writer(nonCoreUser + File.separator + type
                        + FileSuffixConstants.TXT)){
                    for (Long uid: uidSet){
                        csvPrinter.printRecord(uid);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            log.info("处理文件{}完成, 共生成{}条边", filePath, countEdge);
        } catch (IOException e) {
            log.error("处理文件" + filePath + "出错", e);
        }
    }

    private void addToNonCoreUserMap(Map<String, Set<Long>> map, String type, Long uid){
        if (map.containsKey(type)){
            Set<Long> uidSet = map.get(type);
            uidSet.add(uid);
        } else {
            Set<Long> set = new HashSet<>();
            set.add(uid);
            map.put(type, set);
        }
    }


    public void genFollowingRelationshipFile(String filePath, String resultPath) {
        List<String> filePathList = getAllFilePath(filePath);
        if (filePathList == null || filePathList.size() == 0) {
            return;
        }
        if (filePathList.size() == 1) {
            genFollowingRelationshipFileFromOneFile(filePathList.get(0), resultPath);
        } else {
            CountDownLatch countDownLatch = new CountDownLatch(filePathList.size());
            filePathList.forEach(file -> {
                followingTaskExecutor.execute(() -> {
                    try {
                        genFollowingRelationshipFileFromOneFile(file, resultPath);
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            });
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void genFollowingRelationshipFileFromRemote(String filePath, String resultPath) {
        if (StringUtils.isBlank(filePath)) {
            filePath = defaultFollowingDataDirectory;
        }
        List<String> filePathList = getAllRemoteFilePath(filePath);
        if (filePathList == null || filePathList.size() == 0) {
            return;
        }
        filePathList.forEach(file -> {
            genFollowingRelationshipFileFromRemoteFile(new File(file), resultPath);
        });
    }

    public void mergeFollowingFiles(String resultPath) {
        File followingFile = new File(resultPath + File.separator + FOLLOWING_FILE_DIR);
        if (!followingFile.exists()) {
            return;
        }
        String[] fileList = followingFile.list();
        if (fileList == null || fileList.length == 0) {
            return;
        }
        CountDownLatch mergeCountDownLatch = new CountDownLatch(fileList.length);
        for (String fileName : fileList) {
            followingTaskExecutor.execute(() -> {
                try {
                    mergeFile(followingFile.getPath() + File.separator + fileName, resultPath
                            + File.separator + RESULT_DIR_NAME, fileName);
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


    private List<String> getAllRemoteFilePath(String parentPath) {
        LinuxAction linuxAction = LinuxUtil.getSingletonLinuxAction(followingServerIp, followingServerUser, followingServerPassword);
        ResultApi<List<String>> resultApi = linuxAction.executeSuccess("ls " + parentPath);
        List<String> data = resultApi.getData();
        if (data == null || data.size() == 0) {
            return Collections.emptyList();
        }
        if (data.size() == 1 && StringUtils.contains(data.get(0), parentPath)) {
            return Collections.singletonList(data.get(0));
        }
        List<String> fileList = new ArrayList<>();
        for (String fileName : data) {
            fileList.addAll(getAllRemoteFilePath(parentPath + CommonConstants.FORWARD_SLASH + fileName));
        }
        return fileList;
    }

    private List<String> getAllFilePath(String filePath) {
        File oriFile = new File(filePath);
        if (oriFile.isDirectory()) {
            String[] files = oriFile.list();
            if (files == null || files.length == 0) {
                return Collections.emptyList();
            }
            List<String> fileList = new ArrayList<>();
            for (String file : files) {
                fileList.addAll(getAllFilePath(filePath + File.separator + file));
            }
            return fileList;
        }
        return Collections.singletonList(filePath);
    }

    private void genFollowingRelationshipFileFromOneFile(String filePath, String resultPath) {
        File readFile = new File(filePath);
        try (InputStream in = new FileInputStream(readFile)) {
            genFollowingRelationshipFileFromInputStream(in, readFile, resultPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void genFollowingRelationshipFileFromRemoteFile(File file, String resultPath) {
        LinuxAction linuxAction = LinuxUtil.getSingletonLinuxAction(followingServerIp, followingServerUser, followingServerPassword);
        try {
            InputStream in = linuxAction.getInputStream(file.getName(), file.getParent());
            genFollowingRelationshipFileFromInputStream(in, file, resultPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void genFollowingRelationshipFileFromInputStream(InputStream in, File file, String resultPath) {
        String filePath = file.getPath();
        log.info("开始处理文件{}", file.getPath());
        try (BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            int countEdge = 0;
            int infoThresholdStep = 10000;
            int infoThresholdCount = infoThresholdStep;
            String line;
            Map<String, List<FollowingRelationshipDTO>> map = new HashMap<>(16);
            while ((line = br.readLine()) != null) {
                List<FollowingRelationshipDTO> relationshipList;
                try {
                    relationshipList = JSON.parseArray(line, FollowingRelationshipDTO.class);
                } catch (JSONException e) {
                    e.printStackTrace();
                    continue;
                }
                for (FollowingRelationshipDTO followingRelationship : relationshipList) {
                    Long master = followingRelationship.getTo();
                    UserInfo toUserInfo = redisDataManager.getUserInfoViaCache(String.valueOf(master));
                    if (toUserInfo == null || !toUserInfo.isImportantUser()) {
                        continue;
                    }
                    Long slave = followingRelationship.getFrom();
                    UserInfo fromUserInfo = redisDataManager.getUserInfoViaCache(String.valueOf(slave));
                    String fromUserType = UserInfoUtils.getUserType(fromUserInfo);
                    String toUserType = UserInfoUtils.getUserType(toUserInfo);
                    if (StringUtils.isNotBlank(fromUserType) && StringUtils.isNotBlank(toUserType)) {
                        String relationship = fromUserType + RELATION_FOLLOWING + toUserType;
                        if (map.containsKey(relationship)) {
                            map.get(relationship).add(followingRelationship);
                        } else {
                            List<FollowingRelationshipDTO> followingRelationshipList = new ArrayList<>();
                            followingRelationshipList.add(followingRelationship);
                            map.put(relationship, followingRelationshipList);
                        }
                    }
                }
                File fileDir = new File(resultPath + File.separator + FOLLOWING_FILE_DIR);
                fileDir.mkdir();
                countEdge += writeToFile(map, fileDir.getPath(), file.getParentFile().getName()
                        + CommonConstants.UNDERLINE + file.getName());
                if (countEdge >= infoThresholdCount) {
                    infoThresholdCount = (countEdge / infoThresholdStep + 1) * infoThresholdStep;
                    log.info("处理文件{},已生成{}条边", filePath, countEdge);
                }
                map.clear();
            }
            log.info("处理文件{}完成, 共生成{}条边", filePath, countEdge);
        } catch (IOException e) {
            log.error("处理文件" + filePath + "出错", e);
        }
    }

    private int writeToFile(Map<String, List<FollowingRelationshipDTO>> map, String resultDic, String fileSuffix) {
        int[] resultCount = {0};
        map.forEach((key, followingList) -> {
            resultCount[0] += followingList.size();
            String followingFileDicName = resultDic + File.separator + key;
            File followingFileDir = new File(followingFileDicName);
            followingFileDir.mkdir();
            String filePath = followingFileDicName + File.separator + key + CommonConstants.UNDERLINE + fileSuffix
                    + FileSuffixConstants.CSV;
            try {
                writeToCsvFile(followingList, filePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        return resultCount[0];
    }

    private void writeToCsvFile(List<FollowingRelationshipDTO> content, String filePath) throws IOException {
        File file = new File(filePath);
        if (file.exists()) {
            try (CSVPrinter printer = CsvFileHelper.writer(filePath, true)) {
                for (FollowingRelationshipDTO followingRelationship : content) {
                    printer.printRecord(followingRelationship.getFrom(), followingRelationship.getTo(), StringUtils.EMPTY);
                }
            }
        } else {
            try (CSVPrinter printer = CsvFileHelper.writer(filePath, HEADER_FOLLOWING)) {
                for (FollowingRelationshipDTO followingRelationship : content) {
                    printer.printRecord(followingRelationship.getFrom(), followingRelationship.getTo(), StringUtils.EMPTY);
                }
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
            CsvFile csvFile = CsvFile.build(resultDirPath, resultFileName).withHeader(HEADER_FOLLOWING);
            csvFileManager.mergeFile(filePathList, csvFile);
        }
    }
}
