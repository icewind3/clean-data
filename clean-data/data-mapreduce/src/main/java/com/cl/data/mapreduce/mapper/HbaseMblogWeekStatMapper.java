package com.cl.data.mapreduce.mapper;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @author yejianyu
 * @date 2019/10/23
 */
@Slf4j
public class HbaseMblogWeekStatMapper extends TableMapper<Text, NullWritable> {


    private static final String BLOG_HBASE_QUALIFIER_CREATED_AT = "created_at";
    private static final String BLOG_HBASE_QUALIFIER_IS_RETWEETED = "is_retweeted";
    private static final String BLOG_HBASE_QUALIFIER_ATTITUDES_COUNT = "attitudes_count";
    private static final String BLOG_HBASE_QUALIFIER_COMMENTS_COUNT = "comments_count";
    private static final String BLOG_HBASE_QUALIFIER_REPOSTS_COUNT = "reposts_count";
    private static final String DATE_REGEX = "^\\d{4}-\\d{1,2}-\\d{1,2}$";

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        String uid = Bytes.toString(key.get());
        if (!StringUtils.isNumeric(uid)) {
            return;
        }
        Cell[] cells = value.rawCells();
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
                String errorMsg = String.format("数字转换错误, uid=%s, mid=%d, key=%s,value=%s", uid, mid, key, value);
                log.error(errorMsg, e);
            }
        }
        if (map.isEmpty()) {
            return;
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
                    context.write(new Text(blogWeekInfo.toString()), NullWritable.get());
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
            context.write(new Text(blogWeekInfo.toString()), NullWritable.get());
        }
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

    private Timestamp transDateTimeStringToTimestamp(String dateTime) {
        if (Pattern.matches(DATE_REGEX, dateTime)) {
            dateTime = dateTime + " 00:00:00";
        }
        return Timestamp.valueOf(dateTime);
    }

    @Getter
    @Setter
    class BlogWeekInfo {
        long uid;
        long blogTotal = 0L;
        long retweetBlogTotal = 0L;

        long attitudeSum = 0L;
        long commentSum = 0L;
        long repostSum = 0L;

        long repostBlogAttitudeSum = 0L;
        long repostBlogCommentSum = 0L;
        long repostBlogRepostSum = 0L;
        String startDate;
        String endDate;

        BlogWeekInfo(long uid) {
            this.uid = uid;
        }

        void addAttitudeCount(int count, boolean isRetweeted) {
            if (isRetweeted) {
                repostBlogAttitudeSum += count;
            }
            attitudeSum += count;
        }

        void addCommentCount(int count, boolean isRetweeted) {
            if (isRetweeted) {
                repostBlogCommentSum += count;
            }
            commentSum += count;
        }

        void addRepostCount(int count, boolean isRetweeted) {
            if (isRetweeted) {
                repostBlogRepostSum += count;
            }
            repostSum += count;
        }

        void increaseBlogCount(boolean isRetweeted) {
            if (isRetweeted) {
                retweetBlogTotal++;
            }
            blogTotal++;
        }

        void setDateRange(Timestamp createTime) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(createTime.getTime());
            int dayWeek = calendar.get(Calendar.DAY_OF_WEEK);
            if (dayWeek == 1) {
                dayWeek = 7;
            } else {
                dayWeek -= 1;
            }
            // 计算本周开始的时间
            calendar.add(Calendar.DAY_OF_MONTH, 1 - dayWeek);
            Date startDate = calendar.getTime();
            calendar.add(Calendar.DAY_OF_MONTH, 6);
            Date endDate = calendar.getTime();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            this.startDate = sdf.format(startDate);
            this.endDate = sdf.format(endDate);
        }

        @Override
        public String toString() {
            return uid + "," + startDate + "," + endDate + "," + blogTotal + "," + retweetBlogTotal + "," + attitudeSum
                + "," + commentSum + "," + repostSum + "," + repostBlogAttitudeSum + "," + repostBlogCommentSum + ","
                + repostBlogRepostSum;
        }
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
