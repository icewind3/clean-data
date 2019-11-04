package com.cl.data.mapreduce.reducer;

import com.cl.data.mapreduce.dto.BlogInfoDTO;
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
public class UserBlogMedianReducer extends Reducer<Text, BlogInfoDTO, Text, NullWritable> {

    private static final String DATE_REGEX = "^\\d{4}-\\d{1,2}-\\d{1,2}$";
    private static final int MEDIAN_SIZE = 30;

    private MultipleOutputs<Text, NullWritable> multipleOutputs;

    @Override
    protected void reduce(Text key, Iterable<BlogInfoDTO> values, Context context)
        throws IOException, InterruptedException {

        String uid = key.toString();

        Set<String> midSet = new HashSet<>();

        List<MidCount> attitudeList = new ArrayList<>();
        List<MidCount> commentList = new ArrayList<>();
        List<MidCount> repostList = new ArrayList<>();
        int count = 0;
        for (BlogInfoDTO value : values) {
            String mid = value.getMid().toString();
            if (!midSet.add(mid)) {
                continue;
            }
            count++;
            String createdAt = value.getCreatedAt().toString();
            try {
                Timestamp timestamp = transDateTimeStringToTimestamp(createdAt);
                long attitudeCount = value.getAttitudeCount().get();
                long commentCount = value.getCommentCount().get();
                long repostCount = value.getRepostCount().get();
                attitudeList.add(new MidCount(mid, attitudeCount, timestamp));
                commentList.add(new MidCount(mid, commentCount, timestamp));
                repostList.add(new MidCount(mid, repostCount, timestamp));

            } catch (IllegalArgumentException e) {
                log.error("时间格式转换错误：uid={}, mid={}, dateTime={}", key, mid, createdAt);
            }

        }
        writeList(uid, getMedianMidList(attitudeList, MEDIAN_SIZE), "attitude");
        writeList(uid, getMedianMidList(commentList, MEDIAN_SIZE), "comment");
        writeList(uid, getMedianMidList(repostList, MEDIAN_SIZE), "repost");
//        multipleOutputs.write(new Text(uid + "," + count), NullWritable.get(), "uid_blog_count/data");
    }

    private void writeList(String uid, List<String> list, String type) throws IOException, InterruptedException {
        for (String mid : list) {
            String result = uid + "," + mid;
            multipleOutputs.write(new Text(result), NullWritable.get(), "uid_mid_median" + MEDIAN_SIZE +
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

    class MidCount implements Comparable<MidCount> {
        String mid;
        long count;
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