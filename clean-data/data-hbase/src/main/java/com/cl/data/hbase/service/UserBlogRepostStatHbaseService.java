package com.cl.data.hbase.service;

import com.cl.data.hbase.dto.UidMidBlogDTO;
import com.cl.data.hbase.dto.UserBlogInfoDTO;
import com.cl.data.hbase.dto.UserBlogRepostStatDTO;
import com.cl.data.hbase.entity.MblogFromUid;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @author yejianyu
 * @date 2019/8/9
 */
@Slf4j
@Service
public class UserBlogRepostStatHbaseService {

    @Value(value = "${hbase.table-name.mblog}")
    private String blogHbaseTableName;

    private static final String BLOG_HBASE_FAMILY_WEIBO = "weibo";
    private static final String BLOG_HBASE_QUALIFIER_ATTITUDES_COUNT = "attitudes_count";
    private static final String BLOG_HBASE_QUALIFIER_COMMENTS_COUNT = "comments_count";
    private static final String BLOG_HBASE_QUALIFIER_REPOSTS_COUNT = "reposts_count";
    private static final String BLOG_HBASE_QUALIFIER_CREATED_AT = "created_at";
    private static final String BLOG_HBASE_QUALIFIER_MID = "mid";
    private static final String BLOG_HBASE_QUALIFIER_BID = "bid";
    private static final String BLOG_HBASE_QUALIFIER_IS_RETWEETED = "is_retweeted";
    private static final String BLOG_HBASE_QUALIFIER_TEXT = "text";
    private static final String BLOG_HBASE_QUALIFIER_RETWEETED_TEXT = "retweeted_text";
    private static final String DATE_REGEX = "^\\d{4}-\\d{1,2}-\\d{1,2}$";

    @Autowired
    private HbaseTemplate hbaseTemplate;


    public Map<Long, UserBlogRepostStatDTO> getUserBlogRepostStatMap(List<Long> uidList) {
        Result[] results = hbaseTemplate.execute(blogHbaseTableName, table -> {
            List<Get> getList = new ArrayList<>();
            uidList.forEach(uid -> {
                Get get = createUserBlogRepostStatGet(uid);
                getList.add(get);
            });
            return table.get(getList);
        });
        int initialCapacity = (int) (uidList.size() / 0.75f) + 1;
        Map<Long, UserBlogRepostStatDTO> map = new HashMap<>(initialCapacity);
        for (Result result : results) {
            UserBlogRepostStatDTO userBlogRepostStat = getUserBlogRepostStatFromResult(result);
            if (userBlogRepostStat != null) {
                map.put(userBlogRepostStat.getUid(), userBlogRepostStat);
            }
        }
        return map;
    }

    private UserBlogRepostStatDTO getUserBlogRepostStatFromResult(Result result) {
        Cell[] cells = result.rawCells();
        if (cells.length == 0) {
            return null;
        }
        String row = Bytes.toString(result.getRow());
        Long uid = Long.parseLong(row);
        int blogCount = 0;
        int blogRepostCount = 0;
        for (Cell cell : cells) {
            String key = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            if (BLOG_HBASE_QUALIFIER_IS_RETWEETED.equals(key)){
                blogCount++;
                if ("1".equals(value)) {
                    blogRepostCount++;
                }
            }
        }
        UserBlogRepostStatDTO userBlogRepostStat = new UserBlogRepostStatDTO(uid);
        userBlogRepostStat.setBlogCount(blogCount);
        userBlogRepostStat.setBlogCount(blogRepostCount);
        float repostRatio = 0f;
        if (blogCount >0) {
            repostRatio = (float) blogRepostCount / blogCount;
        }
        userBlogRepostStat.setRepostRatio(repostRatio);

        return userBlogRepostStat;
    }

    private Get createUserBlogRepostStatGet(Long uid) {
        Get get = new Get(Bytes.toBytes(String.valueOf(uid)));
        get.setMaxVersions();
//        get.addColumn(Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO), Bytes.toBytes(BLOG_HBASE_QUALIFIER_CREATED_AT));
        get.addColumn(Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO), Bytes.toBytes(BLOG_HBASE_QUALIFIER_IS_RETWEETED));
        return get;
    }

}
