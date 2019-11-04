package com.cl.data.mapreduce.reducer;

import com.cl.data.mapreduce.dto.BlogInfoDTO;
import com.cl.data.mapreduce.util.GsonUtil;
import com.cl.data.mapreduce.util.UserTypeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.net.URI;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @author yejianyu
 * @date 2019/8/20
 */
@Slf4j
public class UserBlogTopReducer extends Reducer<Text, BlogInfoDTO, Text, NullWritable> {

    private static final String DATE_REGEX = "^\\d{4}-\\d{1,2}-\\d{1,2}$";
    private static final int TOP_SIZE = 50;

    private MultipleOutputs<Text, NullWritable> multipleOutputs;

    @Override
    protected void reduce(Text key, Iterable<BlogInfoDTO> values, Context context)
        throws IOException, InterruptedException {

        String uid = key.toString();

        Set<String> midSet = new HashSet<>();

        List<MidCount> attitudeTopList = new ArrayList<>();
        List<MidCount> commentTopList = new ArrayList<>();
        List<MidCount> repostTopList = new ArrayList<>();
//        int size = 0;
//        List<String> blogInfoList = new ArrayList<>();
//        for (BlogInfoDTO ignored : values) {
//            blogInfoList.add(GsonUtil.getGson().toJson(ignored));
//            size++;
//        }
//        int topSize = (int) (0.1 * size) + 1;
        int count = 0;
        for (BlogInfoDTO value : values) {
//            BlogInfoDTO value = GsonUtil.getGson().fromJson(temp, BlogInfoDTO.class);
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

                addToTopList(attitudeTopList, new MidCount(mid, attitudeCount, timestamp), TOP_SIZE);
                addToTopList(commentTopList, new MidCount(mid, commentCount, timestamp), TOP_SIZE);
                addToTopList(repostTopList, new MidCount(mid, repostCount, timestamp), TOP_SIZE);

            } catch (IllegalArgumentException e) {
                log.error("时间格式转换错误：uid={}, mid={}, dateTime={}", key, mid, createdAt);
            }

        }
        writeTopList(uid, attitudeTopList, "attitude");
        writeTopList(uid, commentTopList, "comment");
        writeTopList(uid, repostTopList, "repost");
        multipleOutputs.write(new Text(uid + "," + count), NullWritable.get(), "uid_blog_count/data");
    }

    private void writeTopList(String uid, List<MidCount> list, String type) throws IOException, InterruptedException {
        for (MidCount midCount : list) {
            String result = uid + "," + midCount.getMid();
            multipleOutputs.write(new Text(result), NullWritable.get(), "uid_mid_top" + TOP_SIZE +
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