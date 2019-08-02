package com.cl.graph.weibo.api.service;

import com.cl.graph.weibo.api.dto.UserRelationDTO;
import com.cl.graph.weibo.api.entity.MblogFromUid;
import com.cl.graph.weibo.api.mapper.marketing.MblogFromUidMapper;
import com.cl.graph.weibo.api.util.CsvFileHelper;
import com.cl.graph.weibo.api.util.UserTypeUtils;
import com.cl.graph.weibo.core.constant.FileSuffixConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * @author yejianyu
 * @date 2019/7/16
 */
@Slf4j
@Service
public class MblogFromUidService {

    @Resource
    private MblogFromUidMapper mblogFromUidMapper;

    private static final String RELATION_RETWEET = "_retweet_";
    private static final String[] HEADER_RETWEET = {"from", "to", "mid", "createTime"};

    public void genRetweetFile(String resultPath, String tableSuffix, String sign) {
        UserTypeUtils.initUserSet();
        int pageSize = 100000;
        long maxCount;
        long startTime = System.currentTimeMillis();
        log.info("开始处理mblog_from_uid_{}", tableSuffix);
        try {
            maxCount = mblogFromUidMapper.countRetweet(tableSuffix);
        } catch (InvalidDataAccessResourceUsageException e) {
            processDatabaseException(e, tableSuffix);
            return;
        }
        log.info("统计mblog_from_uid_{}的转发数为{}, 耗时{}ms", tableSuffix, maxCount,
                System.currentTimeMillis() - startTime);
        int count = 0;
        int pageNum = 1;
        Map<String, List<UserRelationDTO>> map = new HashMap<>(34);
        while (count < maxCount) {
            List<MblogFromUid> blogRetweetList = mblogFromUidMapper.findAllRetweet(tableSuffix, pageNum, pageSize);
            wrapRetweetData(blogRetweetList, map);
            count += blogRetweetList.size();
            pageNum++;
            log.info("mblog_from_uid_{}已处理:{}/{}, 已耗时{}ms", tableSuffix, count, maxCount,
                    System.currentTimeMillis() - startTime);
        }
        File fileDic = new File(resultPath);
        fileDic.mkdirs();
        final int[] resultCount = {0};
        map.forEach((key, retweetList) -> {
            resultCount[0] += retweetList.size();
            String filePath = fileDic.getPath() + File.separator + key + FileSuffixConstants.CSV;
            try {
                writeToCsvFile(retweetList, filePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        log.info("mblog_from_uid_{}已处理完成，实际生成{}条边数据, 共耗时{}ms", tableSuffix, resultCount[0],
                System.currentTimeMillis() - startTime);
    }

    /**
     * 根据内存set判断用户类型
     *
     * @param retweetList
     * @param map
     */
    private void wrapRetweetData(List<MblogFromUid> retweetList, Map<String, List<UserRelationDTO>> map) {
        retweetList.forEach(mblogFromUid -> {

            String fromUid = mblogFromUid.getUid();
            Long fromUidLong = Long.valueOf(fromUid);
            if (!UserTypeUtils.isCoreUser(fromUidLong)) {
                return;
            }

            String mid;
            String pid = mblogFromUid.getPid();
            if (StringUtils.isNotBlank(pid)) {
                mid = pid;
            } else {
                mid = mblogFromUid.getRetweetedMid();
            }
            String toUid = mblogFromUid.getRetweetedUid();
            if (StringUtils.isBlank(toUid)) {
                return;
            }
            Long toUidLong = Long.valueOf(toUid);
            if (!UserTypeUtils.isCoreUser(toUidLong)) {
                return;
            }
            String fromUserType = UserTypeUtils.getUserType(fromUidLong);
            String toUserType = UserTypeUtils.getUserType(toUidLong);

            if (StringUtils.isNotBlank(fromUserType) && StringUtils.isNotBlank(toUserType)) {
                String relationship = fromUserType + RELATION_RETWEET + toUserType;
                UserRelationDTO retweetDTO = new UserRelationDTO(fromUid, toUid);
                retweetDTO.setMid(mid);
                retweetDTO.setCreateTime(mblogFromUid.getCreateTime());
                if (map.containsKey(relationship)) {
                    map.get(relationship).add(retweetDTO);
                } else {
                    List<UserRelationDTO> list = new ArrayList<>();
                    list.add(retweetDTO);
                    map.put(relationship, list);
                }
            }
        });
    }

    private void writeToCsvFile(List<UserRelationDTO> content, String filePath) throws IOException {
        try (CSVPrinter printer = CsvFileHelper.writer(filePath, HEADER_RETWEET)) {
            for (UserRelationDTO retweetDTO : content) {
                printer.printRecord(retweetDTO.getFromUid(), retweetDTO.getToUid(), retweetDTO.getMid(),
                        retweetDTO.getCreateTime());
            }
        }
    }

    private void processDatabaseException(RuntimeException e, String tableSuffix) {
        String message = e.getMessage();
        if (StringUtils.containsIgnoreCase(message, "Table")
                && StringUtils.containsIgnoreCase(message, "doesn't exist")) {
            log.warn("表mblog_from_uid_{}不存在", tableSuffix);
        } else {
            log.error("处理表mblog_from_uid_" + tableSuffix + "出错", e);
        }
    }
}
