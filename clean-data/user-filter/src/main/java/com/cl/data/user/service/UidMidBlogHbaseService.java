package com.cl.data.user.service;

import com.cl.data.user.dto.UserBlogInfoDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author yejianyu
 * @date 2019/8/9
 */
@Slf4j
@Service
public class UidMidBlogHbaseService {

    @Value(value = "${hbase.table-name.mblog}")
    private String blogHbaseTableName;

    private static final String BLOG_HBASE_FAMILY_WEIBO = "weibo";
    private static final String BLOG_HBASE_QUALIFIER_ATTITUDES_COUNT = "attitudes_count";
    private static final String BLOG_HBASE_QUALIFIER_COMMENTS_COUNT = "comments_count";
    private static final String BLOG_HBASE_QUALIFIER_REPOSTS_COUNT = "reposts_count";
    private static final String BLOG_HBASE_QUALIFIER_CREATED_AT = "created_at";
    private static final String BLOG_HBASE_QUALIFIER_MID = "mid";
    private static final String BLOG_HBASE_QUALIFIER_TEXT = "text";
    private static final String BLOG_HBASE_QUALIFIER_RETWEETED_TEXT = "retweeted_text";
    private static final String DATE_REGEX = "^\\d{4}-\\d{1,2}-\\d{1,2}$";
    private static final Timestamp LAST_YEAR_TIME = Timestamp.valueOf(LocalDateTime.now().plusYears(-1L));
    private static final Timestamp LAST_THREE_MONTH = Timestamp.valueOf(LocalDateTime.now().plusMonths(-3L));

    @Autowired
    private HbaseTemplate hbaseTemplate;

    public List<UserBlogInfoDTO> getUserBlogInfoList(String uid) {
        List<Long> midList = getMidList(uid);
        Result result = hbaseTemplate.execute(blogHbaseTableName, table -> {
            Get get = new Get(Bytes.toBytes(uid));
            get.setMaxVersions();
            byte[] family = Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO);
            get.setFilter(new TimestampsFilter(midList));
            get.addColumn(family, Bytes.toBytes(BLOG_HBASE_QUALIFIER_TEXT));
            get.addColumn(family, Bytes.toBytes(BLOG_HBASE_QUALIFIER_RETWEETED_TEXT));
            return table.get(get);
        });
        Map<String, UserBlogInfoDTO> map = new HashMap<>();
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            long timestamp = cell.getTimestamp();
            String mid = String.valueOf(timestamp);
            UserBlogInfoDTO userBlogInfo;
            if (map.containsKey(mid)) {
                userBlogInfo = map.get(mid);
            } else {
                userBlogInfo = new UserBlogInfoDTO();
                map.put(mid, userBlogInfo);
            }
            String key = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            if (StringUtils.equals(key, BLOG_HBASE_QUALIFIER_TEXT)) {
                userBlogInfo.setText(value);
            } else if (StringUtils.equals(key, BLOG_HBASE_QUALIFIER_RETWEETED_TEXT)) {
                userBlogInfo.setRetweetedText(value);
            }
        }
        return new ArrayList<>(map.values());
    }

    private List<Long> getMidList(String uid) {
        Result result = hbaseTemplate.execute(blogHbaseTableName, table -> {
            Get get = new Get(Bytes.toBytes(uid));
            get.setMaxVersions();
            byte[] family = Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO);
            get.addColumn(family, Bytes.toBytes(BLOG_HBASE_QUALIFIER_CREATED_AT));
            return table.get(get);
        });
        Cell[] cells = result.rawCells();
        List<Long> midList = new ArrayList<>();
        for (Cell cell : cells) {
            long mid = cell.getTimestamp();
            String createdAt = Bytes.toString(CellUtil.cloneValue(cell));
            try {
                Timestamp timestamp = transDateTimeStringToTimestamp(createdAt);

                if (timestamp.after(LAST_YEAR_TIME)) {
                    midList.add(mid);
                }
            } catch (IllegalArgumentException e) {
                log.error("时间格式转换错误：uid={}, mid={}, dateTime={}", uid, mid, createdAt);
            }
        }
        return midList;
    }

    private Timestamp transDateTimeStringToTimestamp(String dateTime) {
        if (Pattern.matches(DATE_REGEX, dateTime)) {
            dateTime = dateTime + " 00:00:00";
        }
        return Timestamp.valueOf(dateTime);
    }


}
