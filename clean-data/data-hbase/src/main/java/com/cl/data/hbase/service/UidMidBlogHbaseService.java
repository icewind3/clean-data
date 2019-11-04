package com.cl.data.hbase.service;

import com.cl.data.hbase.dto.UidMidBlogDTO;
import com.cl.data.hbase.dto.UserBlogInfoDTO;
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
public class UidMidBlogHbaseService {

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

    public void insertUidMidBlog(UidMidBlogDTO mblogFromUid) {
        hbaseTemplate.execute(blogHbaseTableName, table -> {
            Put put = createPut(mblogFromUid);
            table.put(put);
            return null;
        });
    }

    public void batchInsertUidMidBlog(List<UidMidBlogDTO> uidMidBlogList) {
        hbaseTemplate.execute(blogHbaseTableName, table -> {
            List<Put> list = new ArrayList<>();
            uidMidBlogList.forEach(uidMidBlog -> {
                Put put = createPut(uidMidBlog);
                list.add(put);
            });
            table.put(list);
            return null;
        });
    }

    public void batchInsertUidMidBid(List<MblogFromUid> mblogFromUidList) {
        hbaseTemplate.execute(blogHbaseTableName, table -> {
            List<Put> list = new ArrayList<>();
            mblogFromUidList.forEach(mblogFromUid -> {
                Put put = createBidPut(mblogFromUid);
                list.add(put);
            });
            table.put(list);
            return null;
        });
    }

    public void batchInsertUidMidIsRetweeted(List<MblogFromUid> mblogFromUidList) {
        hbaseTemplate.execute(blogHbaseTableName, table -> {
            List<Put> list = new ArrayList<>();
            mblogFromUidList.forEach(mblogFromUid -> {
                Put put = createIsRetweetedPut(mblogFromUid);
                list.add(put);
            });
            table.put(list);
            return null;
        });
    }

    public Map<Long, UserBlogInfoDTO> getUserBlogInfoMap(List<Long> uidList) {
        Result[] results = hbaseTemplate.execute(blogHbaseTableName, table -> {
            List<Get> getList = new ArrayList<>();
            uidList.forEach(uid -> {
                Get get = createUserBlogInfoGet(uid);
                getList.add(get);
            });
            return table.get(getList);
        });
        int initialCapacity = (int) (uidList.size() / 0.75f) + 1;
        Map<Long, UserBlogInfoDTO> map = new HashMap<>(initialCapacity);
        for (Result result : results) {
            UserBlogInfoDTO userBlogInfo = getUserBlogInfoFromResult(result);
            if (userBlogInfo != null) {
                map.put(userBlogInfo.getUid(), userBlogInfo);
            }
        }
        return map;
    }

    private UserBlogInfoDTO getUserBlogInfoFromResult(Result result) {
        Cell[] cells = result.rawCells();
        if (cells.length == 0) {
            return null;
        }
        String row = Bytes.toString(result.getRow());
        Long uid = Long.parseLong(row);
        long attitudeSum = 0L;
        long commentSum = 0L;
        long repostSum = 0L;
        int mblogTotal = 0;
        List<String> dateTimeList = new ArrayList<>();
        for (Cell cell : cells) {
            String key = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            long mid = cell.getTimestamp();
            try {
                switch (key) {
                    case BLOG_HBASE_QUALIFIER_ATTITUDES_COUNT:
                        attitudeSum += Long.parseLong(value);
                        break;
                    case BLOG_HBASE_QUALIFIER_COMMENTS_COUNT:
                        commentSum += Long.parseLong(value);
                        break;
                    case BLOG_HBASE_QUALIFIER_REPOSTS_COUNT:
                        repostSum += Long.parseLong(value);
                        break;
                    case BLOG_HBASE_QUALIFIER_MID:
                        mblogTotal++;
                        break;
                    case BLOG_HBASE_QUALIFIER_CREATED_AT:
                        dateTimeList.add(value);
                        break;
                    default:
                        break;
                }
            } catch (NumberFormatException e) {
                String errorMsg = String.format("数字转换错误, uid=%d, mid=%d, key=%s,value=%s", uid, mid, key, value);
                log.error(errorMsg, e);
            }
        }
        UserBlogInfoDTO userBlogInfo = new UserBlogInfoDTO(uid);
        userBlogInfo.setAttitudeSum(attitudeSum);
        userBlogInfo.setCommentSum(commentSum);
        userBlogInfo.setRepostSum(repostSum);
        userBlogInfo.setMblogTotal(mblogTotal);

        if (dateTimeList.size() != 0) {
            Timestamp releaseMblogEarly = Timestamp.valueOf(LocalDateTime.now());
            Timestamp releaseMblogLately = new Timestamp(0L);
            for (String dateTime : dateTimeList) {
                if (Pattern.matches(DATE_REGEX, dateTime)) {
                    dateTime = dateTime + " 00:00:00";
                }
                try {
                    Timestamp timestamp = Timestamp.valueOf(dateTime);
                    if (timestamp.before(releaseMblogEarly)) {
                        releaseMblogEarly = timestamp;
                    }
                    if (timestamp.after(releaseMblogLately)) {
                        releaseMblogLately = timestamp;
                    }
                } catch (IllegalArgumentException e) {
                    log.error("时间格式转换错误：uid={}, dateTime={}", uid, dateTime);
                }
            }
            userBlogInfo.setReleaseMblogEarly(releaseMblogEarly);
            userBlogInfo.setReleaseMblogLately(releaseMblogLately);
        }
        return userBlogInfo;
    }

    private Get createUserBlogInfoGet(Long uid) {
        Get get = new Get(Bytes.toBytes(String.valueOf(uid)));
        get.setMaxVersions();
        get.addColumn(Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO), Bytes.toBytes(BLOG_HBASE_QUALIFIER_ATTITUDES_COUNT));
        get.addColumn(Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO), Bytes.toBytes(BLOG_HBASE_QUALIFIER_COMMENTS_COUNT));
        get.addColumn(Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO), Bytes.toBytes(BLOG_HBASE_QUALIFIER_REPOSTS_COUNT));
        get.addColumn(Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO), Bytes.toBytes(BLOG_HBASE_QUALIFIER_CREATED_AT));
        get.addColumn(Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO), Bytes.toBytes(BLOG_HBASE_QUALIFIER_MID));
        return get;
    }

    private Put createPut(UidMidBlogDTO uidMidBlog) {
        String column = BLOG_HBASE_FAMILY_WEIBO;
        String rowKey = uidMidBlog.getUid();
        String mid = uidMidBlog.getMid();
        long ts = Long.parseLong(mid);
        Put put = new Put(Bytes.toBytes(rowKey));
        addColumn(put, column, BLOG_HBASE_QUALIFIER_ATTITUDES_COUNT, ts, uidMidBlog.getAttitudesCount(), "0");
        addColumn(put, column, BLOG_HBASE_QUALIFIER_COMMENTS_COUNT, ts, uidMidBlog.getCommentsCount(), "0");
        addColumn(put, column, BLOG_HBASE_QUALIFIER_REPOSTS_COUNT, ts, uidMidBlog.getRepostsCount(), "0");
        addColumn(put, column, BLOG_HBASE_QUALIFIER_CREATED_AT, ts, uidMidBlog.getCreateTime(), StringUtils.EMPTY);
        addColumn(put, column, BLOG_HBASE_QUALIFIER_MID, ts, mid, StringUtils.EMPTY);
        addColumn(put, column, BLOG_HBASE_QUALIFIER_TEXT, ts, uidMidBlog.getText(), StringUtils.EMPTY);
        // TODO 粉丝博文无转发
        addColumn(put, column, BLOG_HBASE_QUALIFIER_IS_RETWEETED, ts, uidMidBlog.getIsRetweeted(), "0");
//        addColumn(put, column, BLOG_HBASE_QUALIFIER_RETWEETED_TEXT, ts, uidMidBlog.getRetweetedText(), StringUtils.EMPTY);
        return put;
    }

    private Put createBidPut(MblogFromUid mblogFromUid) {
        String rowKey = mblogFromUid.getUid();
        String mid = mblogFromUid.getMid();
        long ts = Long.parseLong(mid);
        Put put = new Put(Bytes.toBytes(rowKey));
        addColumn(put, BLOG_HBASE_FAMILY_WEIBO, BLOG_HBASE_QUALIFIER_BID, ts, mblogFromUid.getBid(), StringUtils.EMPTY);
        return put;
    }

    private Put createIsRetweetedPut(MblogFromUid mblogFromUid) {
        String rowKey = mblogFromUid.getUid();
        String mid = mblogFromUid.getMid();
        long ts = Long.parseLong(mid);
        Put put = new Put(Bytes.toBytes(rowKey));
        String isRetweetedMid = StringUtils.isNotBlank(mblogFromUid.getRetweetedMid()) ? "1" : "0";
        addColumn(put, BLOG_HBASE_FAMILY_WEIBO, BLOG_HBASE_QUALIFIER_IS_RETWEETED, ts, isRetweetedMid, "0");
        return put;
    }

    public boolean isUserExist(String uid) {
        Result result = hbaseTemplate.execute(blogHbaseTableName, table -> {
            Get get = new Get(Bytes.toBytes(uid));
            get.addColumn(Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO), Bytes.toBytes(BLOG_HBASE_QUALIFIER_MID));
            return table.get(get);
        });
        Cell[] cells = result.rawCells();
        return cells != null && cells.length > 0;
    }

    public Set<String> getNoBlogUser(Set<String> uidSet) {
        Result[] results = hbaseTemplate.execute(blogHbaseTableName, table -> {
            List<Get> list = new ArrayList<>();
            uidSet.forEach(uid -> {
                Get get = new Get(Bytes.toBytes(uid));
                get.addColumn(Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO), Bytes.toBytes(BLOG_HBASE_QUALIFIER_MID));
                list.add(get);
            });
            return table.get(list);
        });
        for (Result result : results) {
            String uid = Bytes.toString(result.getRow());
            if (StringUtils.isBlank(uid)) {
                continue;
            }
            Cell[] cells = result.rawCells();
            if (cells != null && cells.length > 0) {
                uidSet.remove(uid);
            }
        }
        return uidSet;
    }

    private void addColumn(Put put, String family, String qualifier, long ts, Object value, String defaultValue) {
        String realValue;
        if (value == null) {
            realValue = defaultValue;
        } else {
            realValue = String.valueOf(value);
        }
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), ts, Bytes.toBytes(realValue));
    }
}
