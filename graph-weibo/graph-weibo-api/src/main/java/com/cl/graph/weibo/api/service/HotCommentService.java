package com.cl.graph.weibo.api.service;

import com.cl.graph.weibo.api.dto.UserRelationDTO;
import com.cl.graph.weibo.api.entity.HotComment;
import com.cl.graph.weibo.api.manage.CsvFile;
import com.cl.graph.weibo.api.manage.CsvFileManager;
import com.cl.graph.weibo.api.mapper.marketing.HotCommentMapper;
import com.cl.graph.weibo.api.util.CsvFileHelper;
import com.cl.graph.weibo.api.util.UserTypeUtils;
import com.cl.graph.weibo.core.constant.CommonConstants;
import com.cl.graph.weibo.core.constant.FileSuffixConstants;
import com.cl.graph.weibo.core.util.DateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * @author yejianyu
 * @date 2019/7/16
 */
@Slf4j
@Service
public class HotCommentService {

    @Resource
    private HotCommentMapper hotCommentMapper;

    private static final String RELATION_COMMENT = "_comment_";
    private static final String[] HEADER_COMMENT = {"from", "to", "mid", "createTime"};

    private static final DateTimeFormatter DATE_TIME_FORMATTER_INPUT = DateTimeFormatter
            .ofPattern("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);
    private static final DateTimeFormatter DATE_TIME_FORMATTER_OUTPUT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");


    public int genCommentFile(String resultPath, String tableSuffix, String sign) {

        UserTypeUtils.initUserSet();

        int pageSize = 100000;
        long maxCount;
        long startTime = System.currentTimeMillis();
        log.info("开始处理hot_comment_{}", tableSuffix);
        try {
            maxCount = hotCommentMapper.count(tableSuffix);
        } catch (InvalidDataAccessResourceUsageException e) {
            log.warn("表hot_comment_{}不存在", tableSuffix);
            return 0;
        }
        log.info("统计hot_comment_{}的总数为{}, 耗时{}ms", tableSuffix, maxCount,
                System.currentTimeMillis() - startTime);
        int count = 0;
        int pageNum = 1;
        Map<String, List<UserRelationDTO>> map = new HashMap<>(34);
        while (count < maxCount) {
            List<HotComment> commentList = hotCommentMapper.findAll(tableSuffix, pageNum, pageSize);
            wrapCommentDataToMap(commentList, map);
            count += commentList.size();
            pageNum++;
            log.info("hot_comment_{}已处理:{}/{}, 已耗时{}ms", tableSuffix, count, maxCount,
                    System.currentTimeMillis() - startTime);
        }
        File fileDic = new File(resultPath);
        fileDic.mkdirs();
        final int[] resultCount = {0};
        map.forEach((key, commentList) -> {
            resultCount[0] += commentList.size();
            String filePath = fileDic.getPath() + File.separator + key + FileSuffixConstants.CSV;
            try {
                writeToCsvFile(commentList, filePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        log.info("hot_comment_{}已处理完成，实际生成{}条边数据, 共耗时{}ms", tableSuffix, resultCount[0],
                System.currentTimeMillis() - startTime);
        return resultCount[0];
    }

    private void wrapCommentDataToMap(List<HotComment> comments, Map<String, List<UserRelationDTO>> map) {
        comments.forEach(hotComment -> {

            String fromUid = hotComment.getUid();
            Long formUidLong = Long.valueOf(fromUid);
            if (!UserTypeUtils.isCoreUser(formUidLong)) {
                return;
            }

            String mid = hotComment.getMid();
            String toUid = hotComment.getMidUid();

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
                UserRelationDTO commentDTO = new UserRelationDTO(fromUid, toUid);
                commentDTO.setMid(mid);
                TemporalAccessor parse = DATE_TIME_FORMATTER_INPUT.parse(hotComment.getCreateTime());
                String createTime = DATE_TIME_FORMATTER_OUTPUT.format(parse);
                commentDTO.setCreateTime(createTime);

                String relationship = fromUserType + RELATION_COMMENT + toUserType;
                addToMap(map, relationship, commentDTO);
            }
        });
    }

    private void addToMap(Map<String, List<UserRelationDTO>> map, String key, UserRelationDTO commentDTO) {
        if (map.containsKey(key)) {
            map.get(key).add(commentDTO);
        } else {
            List<UserRelationDTO> commentDTOList = new ArrayList<>();
            commentDTOList.add(commentDTO);
            map.put(key, commentDTOList);
        }
    }

    private void writeToCsvFile(List<UserRelationDTO> content, String filePath) throws IOException {
        try (CSVPrinter printer = CsvFileHelper.writer(filePath, HEADER_COMMENT)) {
            for (UserRelationDTO commentDTO : content) {
                printer.printRecord(commentDTO.getFromUid(), commentDTO.getToUid(), commentDTO.getMid(),
                        commentDTO.getCreateTime());
            }
        }
    }

}
