package com.cl.graph.weibo.api.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.cl.graph.weibo.api.dto.FollowingRelationshipDTO;
import com.cl.graph.weibo.api.util.CsvFileHelper;
import com.cl.graph.weibo.api.util.UserTypeUtils;
import com.cl.graph.weibo.core.constant.FileSuffixConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author yejianyu
 * @date 2019/7/18
 */
@Slf4j
@Service
public class FollowingRelationshipService {

    private static final String RELATION_FOLLOWING = "_following_";
    private static final String[] HEADER_FOLLOWING = {"from", "to", "createTime"};

    public void genFollowingRelationshipFile(String filePath, String resultPath, String sign) {
        List<String> filePathList = getAllFilePath(filePath);
        if (filePathList == null || filePathList.size() == 0) {
            return;
        }
        UserTypeUtils.initUserSet();
        File fileDic = new File(filePath);
        String date = fileDic.getName();
        filePathList.forEach(file -> {
            genFollowingRelationshipFileFromOneFile(file, resultPath, date);
        });
    }

    private void genFollowingRelationshipFileFromOneFile(String filePath, String resultPath, String date) {
        File file = new File(filePath);
        log.info("开始处理文件{}", file.getPath());
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
            int countEdge = 0;
            int infoThresholdStep = 100000;
            int infoThresholdCount = infoThresholdStep;
            String line;
            Map<String, List<FollowingRelationshipDTO>> map = new HashMap<>(34);
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
                        continue;
                    }
                    String toUserType = UserTypeUtils.getUserType(toUid);
                    if (StringUtils.isNotBlank(fromUserType) && StringUtils.isNotBlank(toUserType)) {
                        followingRelationship.setCreateTime(date);
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
                File fileDir = new File(resultPath);
                fileDir.mkdirs();
                countEdge += writeToFile(map, fileDir.getPath());
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

    private int writeToFile(Map<String, List<FollowingRelationshipDTO>> map, String resultDic) {
        int[] resultCount = {0};
        map.forEach((key, followingList) -> {
            resultCount[0] += followingList.size();
            String filePath = resultDic + File.separator + key + FileSuffixConstants.CSV;
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
        boolean append = file.exists();
        try (CSVPrinter printer = CsvFileHelper.writer(filePath, HEADER_FOLLOWING, append)) {
            for (FollowingRelationshipDTO followingRelationship : content) {
                printer.printRecord(followingRelationship.getFrom(), followingRelationship.getTo(), followingRelationship.getCreateTime());
            }
        }
    }
}
