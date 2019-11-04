package com.cl.data.hbase.service;

import com.cl.data.hbase.dto.BlogDTO;
import com.cl.data.hbase.dto.UidMidBlogDTO;
import com.cl.data.hbase.dto.UserBlogInfoDTO;
import com.cl.data.hbase.dto.UserBlogListDTO;
import com.cl.data.hbase.entity.MblogFromUid;
import com.google.common.collect.Lists;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
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
public class UserBlogSearchHbaseService {

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

    public Map<Long, UserBlogListDTO> getUserBlogListMap(List<Long> uidList) {
        Result[] results = hbaseTemplate.execute(blogHbaseTableName, table -> {
            List<Get> getList = new ArrayList<>();
            uidList.forEach(uid -> {
                Get get = createUserBlogInfoGet(uid);
                getList.add(get);
            });
            return table.get(getList);
        });
        int initialCapacity = (int) (uidList.size() / 0.75f) + 1;
        Map<Long, UserBlogListDTO> map = new HashMap<>(initialCapacity);
        for (Result result : results) {
            UserBlogListDTO userBlogListDTO = getUserBlogListFromResult(result);
            if (userBlogListDTO != null) {
                map.put(userBlogListDTO.getUid(), userBlogListDTO);
            }
        }
        return map;
    }

    public Map<Long, UserBlogListDTO> getUserBlogMedianListMap(List<Long> uidList) {
        Result[] results = hbaseTemplate.execute(blogHbaseTableName, table -> {
            List<Get> getList = new ArrayList<>();
            uidList.forEach(uid -> {
                Get get = createUserBlogInfoGet(uid);
                getList.add(get);
            });
            return table.get(getList);
        });
        int initialCapacity = (int) (uidList.size() / 0.75f) + 1;
        Map<Long, UserBlogListDTO> map = new HashMap<>(initialCapacity);
        for (Result result : results) {
            UserBlogListDTO userBlogListDTO = getUserBlogMedianListFromResult(result);
            if (userBlogListDTO != null) {
                map.put(userBlogListDTO.getUid(), userBlogListDTO);
            }
        }
        return map;
    }

    public Map<Long, UserBlogListDTO> getUserNewestBlogListMap(List<Long> uidList, int size) {
        Result[] results = hbaseTemplate.execute(blogHbaseTableName, table -> {
            List<Get> getList = new ArrayList<>();
            uidList.forEach(uid -> {
                Get get = createUserBlogInfoGet(uid);
                getList.add(get);
            });
            return table.get(getList);
        });
        int initialCapacity = (int) (uidList.size() / 0.75f) + 1;
        Map<Long, UserBlogListDTO> map = new HashMap<>(initialCapacity);
        for (Result result : results) {
            UserBlogListDTO userBlogListDTO = getUserNewestBlogListFromResult(result, size);
            if (userBlogListDTO != null) {
                map.put(userBlogListDTO.getUid(), userBlogListDTO);
            }
        }
        return map;
    }

    private UserBlogListDTO getUserBlogListFromResult(Result result) {

        int topSize = 50;

        Cell[] cells = result.rawCells();
        if (cells.length == 0) {
            return null;
        }
        String row = Bytes.toString(result.getRow());
        Long uid = Long.parseLong(row);
        Map<Long, Timestamp> timestampMap = new HashMap<>();
        for (Cell cell : cells) {
            String key = Bytes.toString(CellUtil.cloneQualifier(cell));
            if (BLOG_HBASE_QUALIFIER_CREATED_AT.equals(key)) {
                String createdAt = Bytes.toString(CellUtil.cloneValue(cell));
                long mid = cell.getTimestamp();
                try {
                    Timestamp timestamp = transDateTimeStringToTimestamp(createdAt);
                    timestampMap.put(mid, timestamp);
                } catch (IllegalArgumentException e) {
                    log.error("时间格式转换错误：uid={}, mid={}, dateTime={}", uid, cell.getTimestamp(), createdAt);
                }
            }
        }

//        List<MidCount> attitudeTopList = new ArrayList<>();
//        List<MidCount> commentTopList = new ArrayList<>();
//        List<MidCount> repostTopList = new ArrayList<>();

        NavigableSet<MidCount> attitudeTopList =  new TreeSet<>(Comparator.reverseOrder());
        NavigableSet<MidCount> commentTopList =  new TreeSet<>(Comparator.reverseOrder());
        NavigableSet<MidCount> repostTopList =  new TreeSet<>(Comparator.reverseOrder());

        for (Cell cell : cells) {
            String key = Bytes.toString(CellUtil.cloneQualifier(cell));

            if (BLOG_HBASE_QUALIFIER_CREATED_AT.equals(key)){
                continue;
            }
            long mid = cell.getTimestamp();
            Timestamp timestamp = timestampMap.get(mid);
            if (timestamp == null) {
               continue;
            }
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            try {
                switch (key) {
                    case BLOG_HBASE_QUALIFIER_ATTITUDES_COUNT:
                        addToTopSet(attitudeTopList, new MidCount(String.valueOf(mid), Long.parseLong(value), timestamp), topSize);
                        break;
                    case BLOG_HBASE_QUALIFIER_COMMENTS_COUNT:
                        addToTopSet(commentTopList, new MidCount(String.valueOf(mid), Long.parseLong(value), timestamp), topSize);
                        break;
                    case BLOG_HBASE_QUALIFIER_REPOSTS_COUNT:
                        addToTopSet(repostTopList, new MidCount(String.valueOf(mid), Long.parseLong(value), timestamp), topSize);
                        break;
                    default:
                        break;
                }
            } catch (NumberFormatException e) {
                String errorMsg = String.format("数字转换错误, uid=%d, mid=%d, key=%s,value=%s", uid, mid, key, value);
                log.error(errorMsg, e);
            }
        }

        UserBlogListDTO userBlogListDTO = new UserBlogListDTO(uid);
//        userBlogListDTO.setAttitudeBlogList(getBlogList(uid, getMidList(attitudeTopList)));
//        userBlogListDTO.setCommentBlogList(getBlogList(uid, getMidList(commentTopList)));
//        userBlogListDTO.setRepostBlogList(getBlogList(uid, getMidList(repostTopList)));
        userBlogListDTO.setAttitudeBlogList(getBlogMidList(uid, getMidList(attitudeTopList)));
        userBlogListDTO.setCommentBlogList(getBlogMidList(uid, getMidList(commentTopList)));
        userBlogListDTO.setRepostBlogList(getBlogMidList(uid, getMidList(repostTopList)));

        return userBlogListDTO;
    }

    /**
     * 获取最新的多条博文
     * @param result
     * @param size 博文条数
     * @return
     */
    private UserBlogListDTO getUserNewestBlogListFromResult(Result result, int size) {
        Cell[] cells = result.rawCells();
        if (cells.length == 0) {
            return null;
        }
        String row = Bytes.toString(result.getRow());
        Long uid = Long.parseLong(row);
        Map<Long, Timestamp> timestampMap = new HashMap<>();
        for (Cell cell : cells) {
            String key = Bytes.toString(CellUtil.cloneQualifier(cell));
            if (BLOG_HBASE_QUALIFIER_CREATED_AT.equals(key)) {
                String createdAt = Bytes.toString(CellUtil.cloneValue(cell));
                long mid = cell.getTimestamp();
                try {
                    Timestamp timestamp = transDateTimeStringToTimestamp(createdAt);
                    timestampMap.put(mid, timestamp);
                } catch (IllegalArgumentException e) {
                    log.error("时间格式转换错误：uid={}, mid={}, dateTime={}", uid, cell.getTimestamp(), createdAt);
                }
            }
        }

        List<Map.Entry<Long, Timestamp>> list = new LinkedList<>(timestampMap.entrySet());
        list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

        Set<Long> midSet = new HashSet<>();
        int i = 0;
        for (Map.Entry<Long, Timestamp> midTimestampEntry : list) {
            midSet.add(midTimestampEntry.getKey());
            i++;
            if (i >= size){
                break;
            }
        }
        Map<Long, BlogDTO> map = new HashMap<>();
        for (Cell cell : cells) {
            String key = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            long mid = cell.getTimestamp();
            if (!midSet.contains(mid)) {
                continue;
            }
            BlogDTO blogDTO;
            if (map.containsKey(mid)) {
                blogDTO = map.get(mid);
            } else {
                blogDTO = new BlogDTO();
                blogDTO.setUid(row);
                map.put(mid, blogDTO);
            }
            switch (key) {
                case BLOG_HBASE_QUALIFIER_TEXT:
                    blogDTO.setText(value);
                    break;
                case BLOG_HBASE_QUALIFIER_RETWEETED_TEXT:
                    blogDTO.setRetweetedText(value);
                    break;
                default:
                    break;
            }
        }

        UserBlogListDTO userBlogListDTO = new UserBlogListDTO(uid);
        userBlogListDTO.setBlogList(new ArrayList<>(map.values()));

        return userBlogListDTO;
    }

    private UserBlogListDTO getUserBlogMedianListFromResult(Result result) {

        int blogSize = 30;

        Cell[] cells = result.rawCells();
        if (cells.length == 0) {
            return null;
        }
        String row = Bytes.toString(result.getRow());
        Long uid = Long.parseLong(row);
        Map<Long, Timestamp> timestampMap = new HashMap<>();
        for (Cell cell : cells) {
            String key = Bytes.toString(CellUtil.cloneQualifier(cell));
            if (BLOG_HBASE_QUALIFIER_CREATED_AT.equals(key)) {
                String createdAt = Bytes.toString(CellUtil.cloneValue(cell));
                long mid = cell.getTimestamp();
                try {
                    Timestamp timestamp = transDateTimeStringToTimestamp(createdAt);
                    timestampMap.put(mid, timestamp);
                } catch (IllegalArgumentException e) {
                    log.error("时间格式转换错误：uid={}, mid={}, dateTime={}", uid, cell.getTimestamp(), createdAt);
                }
            }
        }

        List<MidCount> attitudeList = new ArrayList<>();
        List<MidCount> commentList = new ArrayList<>();
        List<MidCount> repostList = new ArrayList<>();
        for (Cell cell : cells) {
            String key = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            long mid = cell.getTimestamp();
            try {
                switch (key) {
                    case BLOG_HBASE_QUALIFIER_ATTITUDES_COUNT:
                        attitudeList.add(new MidCount(String.valueOf(mid), Long.parseLong(value), timestampMap.get(mid)));
                        break;
                    case BLOG_HBASE_QUALIFIER_COMMENTS_COUNT:
                        commentList.add(new MidCount(String.valueOf(mid), Long.parseLong(value), timestampMap.get(mid)));
                        break;
                    case BLOG_HBASE_QUALIFIER_REPOSTS_COUNT:
                        repostList.add(new MidCount(String.valueOf(mid), Long.parseLong(value), timestampMap.get(mid)));
                        break;
                    default:
                        break;
                }
            } catch (NumberFormatException e) {
                String errorMsg = String.format("数字转换错误, uid=%d, mid=%d, key=%s,value=%s", uid, mid, key, value);
                log.error(errorMsg, e);
            }
        }

        UserBlogListDTO userBlogListDTO = new UserBlogListDTO(uid);
        userBlogListDTO.setAttitudeBlogList(getBlogMidList(uid, getMedianMidList(attitudeList, blogSize)));
        userBlogListDTO.setCommentBlogList(getBlogMidList(uid, getMedianMidList(commentList, blogSize)));
        userBlogListDTO.setRepostBlogList(getBlogMidList(uid, getMedianMidList(repostList, blogSize)));

        return userBlogListDTO;
    }


    public List<BlogDTO> getBlogMidList(Long uid, List<String> midList) {
        List<BlogDTO> list = new ArrayList<>();
        midList.forEach(mid -> {
            BlogDTO blogDTO = new BlogDTO();
            blogDTO.setUid(String.valueOf(uid));
            blogDTO.setMid(mid);
            list.add(blogDTO);
        });
        return list;
    }

    private List<String> getMedianMidList(List<MidCount> list, int size) {
        list.sort(Collections.reverseOrder());
        List<String> medianMidList = new ArrayList<>();
        int length = list.size();
        if (length <= size) {
            list.forEach(midCount -> {
                medianMidList.add(midCount.getMid());
            });
            return medianMidList;
        }
        int start = Math.max(((length + 1) / 2) - (size / 2), 0);
        int end = Math.min(length, start + size);
        for (int i = start; i < end; i++) {
            medianMidList.add(list.get(i).getMid());
        }
        return medianMidList;
    }


    public List<BlogDTO> getBlogList(Long uid, List<String> midList) {
        Result result = hbaseTemplate.execute(blogHbaseTableName, table -> {
            Get get = createUserBlogTextGet(uid, midList);
            return table.get(get);
        });
        Cell[] cells = result.rawCells();
        Map<Long, BlogDTO> map = new HashMap<>();
        for (Cell cell : cells) {
            String key = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            long mid = cell.getTimestamp();
            BlogDTO blogDTO;
            if (map.containsKey(mid)) {
                blogDTO = map.get(mid);
            } else {
                blogDTO = new BlogDTO();
                blogDTO.setMid(String.valueOf(mid));
                blogDTO.setUid(String.valueOf(uid));
                map.put(mid, blogDTO);
            }
            if (BLOG_HBASE_QUALIFIER_TEXT.equals(key)) {
                blogDTO.setText(value);
            } else if (BLOG_HBASE_QUALIFIER_RETWEETED_TEXT.equals(key)) {
                blogDTO.setRetweetedText(value);
            }
        }
        return new ArrayList<>(map.values());
    }

    private List<String> getMidList(List<MidCount> midCountList) {
        List<String> midList = new ArrayList<>();
        midCountList.forEach(midCount -> {
            midList.add(midCount.getMid());
        });
        return midList;
    }

    private List<String> getMidList(NavigableSet<MidCount> midCountList) {
        List<String> midList = new ArrayList<>();
        midCountList.forEach(midCount -> {
            midList.add(midCount.getMid());
        });
        return midList;
    }

    private Get createUserBlogInfoGet(Long uid) {
        Get get = new Get(Bytes.toBytes(String.valueOf(uid)));
        get.setMaxVersions();
        get.addColumn(Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO), Bytes.toBytes(BLOG_HBASE_QUALIFIER_ATTITUDES_COUNT));
        get.addColumn(Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO), Bytes.toBytes(BLOG_HBASE_QUALIFIER_COMMENTS_COUNT));
        get.addColumn(Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO), Bytes.toBytes(BLOG_HBASE_QUALIFIER_REPOSTS_COUNT));
        get.addColumn(Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO), Bytes.toBytes(BLOG_HBASE_QUALIFIER_CREATED_AT));
//        get.addColumn(Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO), Bytes.toBytes(BLOG_HBASE_QUALIFIER_MID));
//        get.addColumn(Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO), Bytes.toBytes(BLOG_HBASE_QUALIFIER_TEXT));
//        get.addColumn(Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO), Bytes.toBytes(BLOG_HBASE_QUALIFIER_RETWEETED_TEXT));
        return get;
    }

    private Get createUserBlogTextGet(Long uid, List<String> midList) {
        List<Long> timestampList = new ArrayList<>();
        midList.forEach(mid -> {
            timestampList.add(Long.parseLong(mid));
        });
        Get get = new Get(Bytes.toBytes(String.valueOf(uid)));
        get.setMaxVersions();
        get.setFilter(new TimestampsFilter(timestampList));
        get.addColumn(Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO), Bytes.toBytes(BLOG_HBASE_QUALIFIER_TEXT));
        get.addColumn(Bytes.toBytes(BLOG_HBASE_FAMILY_WEIBO), Bytes.toBytes(BLOG_HBASE_QUALIFIER_RETWEETED_TEXT));
        return get;
    }

    private Timestamp transDateTimeStringToTimestamp(String dateTime) {
        if (Pattern.matches(DATE_REGEX, dateTime)) {
            dateTime = dateTime + " 00:00:00";
        }
        return Timestamp.valueOf(dateTime);
    }

    private void addToTopList(List<MidCount> list, MidCount midCount, int size) {
        int length = list.size();
        if (length < size) {
            list.add(midCount);
            list.sort(Collections.reverseOrder());
        } else if (list.get(size - 1).compareTo(midCount) < 0) {
            list.set(size - 1, midCount);
            list.sort(Collections.reverseOrder());
        }
    }

    private void addToTopSet(NavigableSet<MidCount> list, MidCount midCount, int size) {
        int length = list.size();
        if (length < size) {
            list.add(midCount);
        } else if (list.last().compareTo(midCount) < 0) {
            list.pollLast();
            list.add(midCount);
        }
    }

    @EqualsAndHashCode
    class MidCount implements Comparable<MidCount> {

        String mid;
        long count;

        @EqualsAndHashCode.Exclude
        Timestamp timestamp;

        MidCount(String mid, long count, Timestamp timestamp) {
            this.mid = mid;
            this.count = count;
            this.timestamp = timestamp;
        }

        @Override
        public int compareTo(MidCount o) {
            if (this.count > o.count) {
                return 1;
            }
            if (this.count < o.count) {
                return -1;
            }
            if (this.timestamp.after(o.timestamp)) {
                return 1;
            }
            if (this.timestamp.before(o.timestamp)) {
                return -1;
            }
            return Long.compare(Long.parseLong(this.mid), Long.parseLong(o.mid));
        }

        public String getMid() {
            return mid;
        }

        public void setMid(String mid) {
            this.mid = mid;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }
    }

}
