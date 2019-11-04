package com.cl.data.mapreduce.reducer;

import com.cl.data.mapreduce.dto.BlogInfoDTO;
import com.cl.data.mapreduce.util.GsonUtil;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @author yejianyu
 * @date 2019/8/20
 */
@Slf4j
public class UserBlogTopPercentReducer extends Reducer<Text, BlogInfoDTO, Text, NullWritable> {

    private static final String DATE_REGEX = "^\\d{4}-\\d{1,2}-\\d{1,2}$";
    private static final float TOP_PERCENT = 0.1f;

    private MultipleOutputs<Text, NullWritable> multipleOutputs;

    @Override
    protected void reduce(Text key, Iterable<BlogInfoDTO> values, Context context)
        throws IOException, InterruptedException {

        String uid = key.toString();

        Set<String> midSet = new HashSet<>();

        NavigableSet<MidCount> attitudeTopList =  new TreeSet<>(Comparator.reverseOrder());
        NavigableSet<MidCount> commentTopList =  new TreeSet<>(Comparator.reverseOrder());
        NavigableSet<MidCount> repostTopList =  new TreeSet<>(Comparator.reverseOrder());

        int size = 0;
        List<String> blogInfoList = new ArrayList<>();
        for (BlogInfoDTO ignored : values) {
            if (!midSet.add(ignored.getMid().toString())) {
                continue;
            }
            blogInfoList.add(GsonUtil.getGson().toJson(ignored));
            size++;
        }
        int topSize = (int) (TOP_PERCENT * size) + 1;
        int count = 0;
        for (String temp : blogInfoList) {
            BlogInfoDTO value = GsonUtil.getGson().fromJson(temp, BlogInfoDTO.class);
            String mid = value.getMid().toString();
            count++;
            String createdAt = value.getCreatedAt().toString();
            try {
                Timestamp timestamp = transDateTimeStringToTimestamp(createdAt);
                long attitudeCount = value.getAttitudeCount().get();
                long commentCount = value.getCommentCount().get();
                long repostCount = value.getRepostCount().get();

                addToTopList(attitudeTopList, new MidCount(mid, attitudeCount, timestamp), topSize);
                addToTopList(commentTopList, new MidCount(mid, commentCount, timestamp), topSize);
                addToTopList(repostTopList, new MidCount(mid, repostCount, timestamp), topSize);

            } catch (IllegalArgumentException e) {
                log.error("时间格式转换错误：uid={}, mid={}, dateTime={}", key, mid, createdAt);
            }

        }
        writeTopList(uid, attitudeTopList, "attitude");
        writeTopList(uid, commentTopList, "comment");
        writeTopList(uid, repostTopList, "repost");
        multipleOutputs.write(new Text(uid + "," + count), NullWritable.get(), "uid_blog_count/data");
    }

    private void writeTopList(String uid, NavigableSet<MidCount> list, String type) throws IOException, InterruptedException {
        for (MidCount midCount : list) {
            String result = uid + "," + midCount.getMid();
            multipleOutputs.write(new Text(result), NullWritable.get(), "uid_mid_top_10percent" +
                "_" + type + "/data");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }

    @Override
    protected void setup(Context context) throws IOException {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    private Timestamp transDateTimeStringToTimestamp(String dateTime) {
        if (Pattern.matches(DATE_REGEX, dateTime)) {
            dateTime = dateTime + " 00:00:00";
        }
        return Timestamp.valueOf(dateTime);
    }

    private void addToTopList(NavigableSet<MidCount> list, MidCount midCount, int size) {
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