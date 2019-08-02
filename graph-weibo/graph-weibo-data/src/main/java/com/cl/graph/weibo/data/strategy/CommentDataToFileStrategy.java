package com.cl.graph.weibo.data.strategy;

import com.cl.graph.weibo.data.dto.CommentDTO;
import com.cl.graph.weibo.data.entity.HotComment;
import com.cl.graph.weibo.data.entity.UserInfo;
import com.cl.graph.weibo.data.manager.RedisDataManager;
import com.cl.graph.weibo.data.util.CsvFileHelper;
import com.cl.graph.weibo.data.util.UserInfoUtils;
import com.cl.graph.weibo.data.util.UserTypeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * @author yejianyu
 * @date 2019/7/26
 */
@Slf4j
@Component
public class CommentDataToFileStrategy implements DataToFileStrategy<HotComment, CommentDTO> {

    private final RedisDataManager redisDataManager;

    protected static final String[] HEADER_COMMENT = {"from", "to", "mid", "createTime"};
    private static final DateTimeFormatter DATE_TIME_FORMATTER_INPUT = DateTimeFormatter
            .ofPattern("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);
    private static final DateTimeFormatter DATE_TIME_FORMATTER_OUTPUT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final String RELATION_COMMENT = "_comment_";

    public CommentDataToFileStrategy(RedisDataManager redisDataManager) {
        this.redisDataManager = redisDataManager;
    }

    @Override
    public void wrapDataToMap(List<HotComment> list, Map<String, List<CommentDTO>> map) {
        list.forEach(hotComment -> {

            String fromUid = hotComment.getUid();
            UserInfo fromUserInfo = redisDataManager.getUserInfoViaCache(fromUid);
            if (fromUserInfo == null || !fromUserInfo.isCoreUser()) {
                return;
            }
            String mid = hotComment.getMid();
            String toUid = redisDataManager.getUidByMid(mid);
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
                String relationship = fromUserType + RELATION_COMMENT + toUserType;
                CommentDTO commentDTO = new CommentDTO(fromUid, toUid);
                commentDTO.setMid(mid);
                String createTime;
                try {
                    TemporalAccessor parse = DATE_TIME_FORMATTER_INPUT.parse(hotComment.getCreateTime());
                    createTime = DATE_TIME_FORMATTER_OUTPUT.format(parse);
                } catch (DateTimeParseException e) {
                    String error = String.format("r_mid = %s的评论日期转换出错：%s", hotComment.getRMid(), hotComment.getCreateTime());
                    log.error(error, e);
                    return;
                }
                commentDTO.setCreateTime(createTime);
                if (map.containsKey(relationship)) {
                    map.get(relationship).add(commentDTO);
                } else {
                    List<CommentDTO> commentDTOList = new ArrayList<>();
                    commentDTOList.add(commentDTO);
                    map.put(relationship, commentDTOList);
                }
            }
        });
    }



    @Override
    public void writeToCsvFile(List<CommentDTO> content, String filePath) throws IOException {
        try (CSVPrinter printer = CsvFileHelper.writer(filePath, HEADER_COMMENT)) {
            for (CommentDTO commentDTO : content) {
                printer.printRecord(commentDTO.getFromUid(), commentDTO.getToUid(), commentDTO.getMid(),
                        commentDTO.getCreateTime());
            }
        }
    }

//    protected CommentDTO genResult(HotComment hotComment){
//        String fromUid = hotComment.getUid();
//        UserInfo fromUserInfo = redisDataManager.getUserInfoViaCache(fromUid);
//        if (fromUserInfo == null || !fromUserInfo.isCoreUser()) {
//            return null;
//        }
//        String mid = hotComment.getMid();
//        String toUid = redisDataManager.getUidByMid(mid);
//        if (StringUtils.isBlank(toUid)) {
//            return;
//        }
//        UserInfo toUserInfo = redisDataManager.getUserInfoViaCache(toUid);
//        if (toUserInfo == null || !toUserInfo.isCoreUser()) {
//            return;
//        }
//        String fromUserType = UserInfoUtils.getUserType(fromUserInfo);
//        String toUserType = UserInfoUtils.getUserType(toUserInfo);
//    }
}
