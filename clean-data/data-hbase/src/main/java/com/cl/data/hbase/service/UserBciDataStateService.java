package com.cl.data.hbase.service;

import com.cl.data.hbase.dto.UserBlogInfoDTO;
import com.cl.data.hbase.entity.BlogWeekInfo;
import com.cl.data.hbase.entity.UserBlogState;
import com.cl.data.hbase.mapper.marketing.UidCoreMapper;
import com.cl.data.hbase.mapper.weibo.UserBlogStateMapper;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * @author yejianyu
 * @date 2019/9/9
 */
@Slf4j
@Service
public class UserBciDataStateService {

    @Autowired
    private HbaseTemplate hbaseTemplate;

    @Value(value = "${hbase.table-name.mblog}")
    private String blogHbaseTableName;

    @Resource(name = "hbaseThreadPoolTaskExecutor")
    private ThreadPoolTaskExecutor hbaseThreadPoolTaskExecutor;

    private static final String BLOG_HBASE_FAMILY_WEIBO = "weibo";
    private static final String BLOG_HBASE_QUALIFIER_CREATED_AT = "created_at";
    private static final String BLOG_HBASE_QUALIFIER_IS_RETWEETED = "is_retweeted";
    private static final String BLOG_HBASE_QUALIFIER_ATTITUDES_COUNT = "attitudes_count";
    private static final String BLOG_HBASE_QUALIFIER_COMMENTS_COUNT = "comments_count";
    private static final String BLOG_HBASE_QUALIFIER_REPOSTS_COUNT = "reposts_count";
    private static final String DATE_REGEX = "^\\d{4}-\\d{1,2}-\\d{1,2}$";

    public List<BlogWeekInfo> getBlogWeekInfoList(List<Long> uidList) {
        Result[] results = hbaseTemplate.execute(blogHbaseTableName, table -> {
            List<Get> getList = new ArrayList<>();
            uidList.forEach(uid -> {
                Get get = createUserBlogInfoGet(uid);
                getList.add(get);
            });
            return table.get(getList);
        });
        List<BlogWeekInfo> resultList = new ArrayList<>();
        for (Result result : results) {
            List<BlogWeekInfo> blogWeekInfoList = getBlogWeekInfoList(result);
            if (!blogWeekInfoList.isEmpty()) {
                resultList.addAll(blogWeekInfoList);
            }
        }
        return resultList;
    }


    private List<BlogWeekInfo> getBlogWeekInfoList(Result result) {
        List<BlogWeekInfo> blogWeekInfoList = new ArrayList<>();
        String uid = Bytes.toString(result.getRow());
        if (!StringUtils.isNumeric(uid)) {
            return blogWeekInfoList;
        }
        Cell[] cells = result.rawCells();
        Map<Long, BlogInfoDTO> map = new HashMap<>();

        for (Cell cell : cells) {
            long mid = cell.getTimestamp();
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String cValue = Bytes.toString(CellUtil.cloneValue(cell));
            BlogInfoDTO blogInfo = getBlogInfo(map, mid);
            try {
                switch (qualifier) {
                    case BLOG_HBASE_QUALIFIER_CREATED_AT:
                        try {
                            Timestamp timestamp = transDateTimeStringToTimestamp(cValue);
                            blogInfo.setCreateTime(timestamp);
                        } catch (IllegalArgumentException e) {
                            log.error("时间格式转换错误：uid={}, mid={}, dateTime={}", uid, cell.getTimestamp(), cValue);
                        }
                        break;
                    case BLOG_HBASE_QUALIFIER_IS_RETWEETED:
                        boolean isRetweeeted = "1".equals(cValue);
                        blogInfo.setIsRetweeted(isRetweeeted);
                        break;
                    case BLOG_HBASE_QUALIFIER_ATTITUDES_COUNT:
                        blogInfo.setAttitudesCount(Integer.parseInt(cValue));
                        break;
                    case BLOG_HBASE_QUALIFIER_COMMENTS_COUNT:
                        blogInfo.setCommentCount(Integer.parseInt(cValue));
                        break;
                    case BLOG_HBASE_QUALIFIER_REPOSTS_COUNT:
                        blogInfo.setRepostsCount(Integer.parseInt(cValue));
                        break;
                    default:
                        break;
                }
            } catch (NumberFormatException e) {
                String errorMsg = String.format("数字转换错误, uid=%s, mid=%d, key=%s,value=%s", uid, mid, qualifier, cValue);
                log.error(errorMsg, e);
            }
        }
        if (map.isEmpty()) {
            return blogWeekInfoList;
        }
        List<BlogInfoDTO> list = new ArrayList<>(map.size());
        for (BlogInfoDTO blogInfoDTO : map.values()) {
            if (blogInfoDTO.getCreateTime() != null && blogInfoDTO.getIsRetweeted() != null) {
                list.add(blogInfoDTO);
            }
        }

        list.sort(BlogInfoDTO::compareTo);

        Timestamp endTime = new Timestamp(0L);
        BlogWeekInfo blogWeekInfo = null;
        for (BlogInfoDTO blogInfoDTO : list) {
            Timestamp createTime = blogInfoDTO.getCreateTime();
            if (createTime == null) {
                continue;
            }
            if (createTime.after(endTime)) {
                if (blogWeekInfo != null) {
                    blogWeekInfoList.add(blogWeekInfo);
                }
                blogWeekInfo = new BlogWeekInfo(Long.parseLong(uid));
                blogWeekInfo.setDateRange(createTime);
                endTime = Timestamp.valueOf(blogWeekInfo.getEndDate() + " 23:59:59");
            }
            Boolean isRetweeted = blogInfoDTO.getIsRetweeted();
            if (isRetweeted == null) {
                isRetweeted = false;
            }
            if (blogWeekInfo == null) {
                log.error("uid = {}, createTime = {},mid = {}", uid, createTime, blogInfoDTO.getMid());
                log.error(blogInfoDTO.toString());
            } else {
                blogWeekInfo.increaseBlogCount(isRetweeted);
                blogWeekInfo.addAttitudeCount(blogInfoDTO.getAttitudesCount(), isRetweeted);
                blogWeekInfo.addCommentCount(blogInfoDTO.getCommentCount(), isRetweeted);
                blogWeekInfo.addRepostCount(blogInfoDTO.getRepostsCount(), isRetweeted);
            }
        }
        if (blogWeekInfo != null) {
            blogWeekInfoList.add(blogWeekInfo);
        }
        return blogWeekInfoList;
    }

    private BlogInfoDTO getBlogInfo(Map<Long, BlogInfoDTO> map, long mid) {
        if (map.containsKey(mid)) {
            return map.get(mid);
        } else {
            BlogInfoDTO blogInfoDTO = new BlogInfoDTO(mid);
            map.put(mid, blogInfoDTO);
            return blogInfoDTO;
        }
    }

    private Get createUserBlogInfoGet(Long uid) {
        Get get = new Get(Bytes.toBytes(String.valueOf(uid)));
        get.setMaxVersions();
        get.addColumn(Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO), Bytes.toBytes(BLOG_HBASE_QUALIFIER_ATTITUDES_COUNT));
        get.addColumn(Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO), Bytes.toBytes(BLOG_HBASE_QUALIFIER_COMMENTS_COUNT));
        get.addColumn(Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO), Bytes.toBytes(BLOG_HBASE_QUALIFIER_REPOSTS_COUNT));
        get.addColumn(Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO), Bytes.toBytes(BLOG_HBASE_QUALIFIER_CREATED_AT));
        get.addColumn(Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO), Bytes.toBytes(BLOG_HBASE_QUALIFIER_IS_RETWEETED));
        return get;
    }

    private Timestamp transDateTimeStringToTimestamp(String dateTime) {
        if (Pattern.matches(DATE_REGEX, dateTime)) {
            dateTime = dateTime + " 00:00:00";
        }
        return Timestamp.valueOf(dateTime);
    }


    @Data
    static class BlogInfoDTO implements Comparable<BlogInfoDTO> {
        private Long mid;
        private Integer attitudesCount;
        private Integer commentCount;
        private Integer repostsCount;
        private Boolean isRetweeted;
        private Timestamp createTime;

        BlogInfoDTO(Long mid) {
            this.mid = mid;
        }

        @Override
        public int compareTo(BlogInfoDTO o) {
            if (this.createTime.after(o.createTime)) {
                return 1;
            }
            if (this.createTime.before(o.createTime)) {
                return -1;
            }
            return this.mid.compareTo(o.mid);
        }
    }


}
