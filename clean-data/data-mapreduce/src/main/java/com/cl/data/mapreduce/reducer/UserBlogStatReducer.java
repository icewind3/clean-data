package com.cl.data.mapreduce.reducer;

import com.cl.data.mapreduce.dto.BlogInfoDTO;
import com.cl.data.mapreduce.dto.UserBlogInfoDTO;
import com.cl.data.mapreduce.util.UserTypeUtils;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
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
public class UserBlogStatReducer extends Reducer<Text, BlogInfoDTO, NullWritable, Text> {

    private static final String DATE_REGEX = "^\\d{4}-\\d{1,2}-\\d{1,2}$";
    private static final Timestamp LAST_YEAR_TIME = Timestamp.valueOf(LocalDateTime.now().plusYears(-1L));
    private static final Timestamp LAST_THREE_MONTH = Timestamp.valueOf(LocalDateTime.now().plusMonths(-3L));
    private static final int ONE_YEAR_DAYS = 365;
    private static final int THREE_MONTH_DAYS = 91;
    private static final int TOP_SIZE = 10;

    private MultipleOutputs<NullWritable, Text> multipleOutputs;

    @Override
    protected void reduce(Text key, Iterable<BlogInfoDTO> values, Context context)
        throws IOException, InterruptedException {

        Set<String> midSet = new HashSet<>();
        int totalRecentlyYear = 0;
        long attitudeSumRecentlyYear = 0;
        long commentSumRecentlyYear = 0;
        long repostSumRecentlyYear = 0;
        int retweetedTotalRecentlyYear = 0;

        int totalRecentlyThreeMonth = 0;
        long attitudeSumRecentlyThreeMonth = 0;
        long commentSumRecentlyThreeMonth = 0;
        long repostSumRecentlyThreeMonth = 0;
        int retweetedTotalRecentlyThreeMonth = 0;

        List<Long> attitudeRecentlyYearList = new ArrayList<>();
        List<Long> commentRecentlyYearList = new ArrayList<>();
        List<Long> repostRecentlyYearList = new ArrayList<>();

        List<Long> attitudeRecentlyThreeMonthList = new ArrayList<>();
        List<Long> commentRecentlyThreeMonthList = new ArrayList<>();
        List<Long> repostRecentlyThreeMonthList = new ArrayList<>();

        List<MidCount> attitudeTopList = new ArrayList<>();
        List<MidCount> commentTopList = new ArrayList<>();
        List<MidCount> repostTopList = new ArrayList<>();

        for (BlogInfoDTO value : values) {
            String mid = value.getMid().toString();
            if (!midSet.add(mid)) {
                continue;
            }
            String createdAt = value.getCreatedAt().toString();
            try {
                Timestamp timestamp = transDateTimeStringToTimestamp(createdAt);
                long attitudeCount = value.getAttitudeCount().get();
                long commentCount = value.getCommentCount().get();
                long repostCount = value.getRepostCount().get();

                addToTopList(attitudeTopList, new MidCount(mid, attitudeCount, timestamp), TOP_SIZE);
                addToTopList(commentTopList, new MidCount(mid, commentCount, timestamp), TOP_SIZE);
                addToTopList(repostTopList, new MidCount(mid, repostCount, timestamp), TOP_SIZE);

                if (LAST_YEAR_TIME.after(timestamp)) {
                    continue;
                }

                int retweetedCount = value.getIsRetweeted().get();

                attitudeRecentlyYearList.add(attitudeCount);
                commentRecentlyYearList.add(commentCount);
                repostRecentlyYearList.add(repostCount);
                attitudeSumRecentlyYear += attitudeCount;
                commentSumRecentlyYear += commentCount;
                repostSumRecentlyYear += repostCount;
                retweetedTotalRecentlyYear += retweetedCount;
                totalRecentlyYear++;

                if (LAST_THREE_MONTH.after(timestamp)) {
                    continue;
                }
                attitudeRecentlyThreeMonthList.add(attitudeCount);
                commentRecentlyThreeMonthList.add(commentCount);
                repostRecentlyThreeMonthList.add(repostCount);
                attitudeSumRecentlyThreeMonth += attitudeCount;
                commentSumRecentlyThreeMonth += commentCount;
                repostSumRecentlyThreeMonth += repostCount;
                retweetedTotalRecentlyThreeMonth += retweetedCount;
                totalRecentlyThreeMonth++;

            } catch (IllegalArgumentException e) {
                log.error("时间格式转换错误：uid={}, mid={}, dateTime={}", key, mid, createdAt);
            }

        }

        UserBlogInfoDTO userBlogInfoDTO = new UserBlogInfoDTO(key);

        if (totalRecentlyYear == 0) {
            userBlogInfoDTO.setReleaseFrequency(new FloatWritable(0));
            userBlogInfoDTO.setAttitudeAvg(new LongWritable(0));
            userBlogInfoDTO.setCommentAvg(new LongWritable(0));
            userBlogInfoDTO.setRepostAvg(new LongWritable(0));
            userBlogInfoDTO.setAttitudeMedian(new LongWritable(0));
            userBlogInfoDTO.setCommentMedian(new LongWritable(0));
            userBlogInfoDTO.setRepostMedian(new LongWritable(0));
            userBlogInfoDTO.setRepostRate(new FloatWritable(0));
        } else {
            userBlogInfoDTO.setReleaseFrequency(new FloatWritable((float) totalRecentlyYear / ONE_YEAR_DAYS));
            userBlogInfoDTO.setAttitudeAvg(new LongWritable(attitudeSumRecentlyYear / totalRecentlyYear));
            userBlogInfoDTO.setCommentAvg(new LongWritable(commentSumRecentlyYear / totalRecentlyYear));
            userBlogInfoDTO.setRepostAvg(new LongWritable(repostSumRecentlyYear / totalRecentlyYear));
            userBlogInfoDTO.setAttitudeMedian(new LongWritable(getMedian(attitudeRecentlyYearList)));
            userBlogInfoDTO.setCommentMedian(new LongWritable(getMedian(commentRecentlyYearList)));
            userBlogInfoDTO.setRepostMedian(new LongWritable(getMedian(repostRecentlyYearList)));
            userBlogInfoDTO.setRepostRate(new FloatWritable((float) retweetedTotalRecentlyYear / totalRecentlyYear));
        }

        if (totalRecentlyThreeMonth == 0) {
            userBlogInfoDTO.setReleaseFrequency2(new FloatWritable(0));
            userBlogInfoDTO.setAttitudeAvg2(new LongWritable(0));
            userBlogInfoDTO.setCommentAvg2(new LongWritable(0));
            userBlogInfoDTO.setRepostAvg2(new LongWritable(0));
            userBlogInfoDTO.setAttitudeMedian2(new LongWritable(0));
            userBlogInfoDTO.setCommentMedian2(new LongWritable(0));
            userBlogInfoDTO.setRepostMedian2(new LongWritable(0));
            userBlogInfoDTO.setRepostRate2(new FloatWritable(0));
        } else {
            userBlogInfoDTO.setReleaseFrequency2(new FloatWritable((float) totalRecentlyThreeMonth / THREE_MONTH_DAYS));
            userBlogInfoDTO.setAttitudeAvg2(new LongWritable(attitudeSumRecentlyThreeMonth / totalRecentlyThreeMonth));
            userBlogInfoDTO.setCommentAvg2(new LongWritable(commentSumRecentlyThreeMonth / totalRecentlyThreeMonth));
            userBlogInfoDTO.setRepostAvg2(new LongWritable(repostSumRecentlyThreeMonth / totalRecentlyThreeMonth));
            userBlogInfoDTO.setAttitudeMedian2(new LongWritable(getMedian(attitudeRecentlyThreeMonthList)));
            userBlogInfoDTO.setCommentMedian2(new LongWritable(getMedian(commentRecentlyThreeMonthList)));
            userBlogInfoDTO.setRepostMedian2(new LongWritable(getMedian(repostRecentlyThreeMonthList)));
            userBlogInfoDTO.setRepostRate2(new FloatWritable((float) retweetedTotalRecentlyThreeMonth / totalRecentlyThreeMonth));
        }

        userBlogInfoDTO.setAttitudeTopAvg(new LongWritable(computeTopAvg(attitudeTopList)));
        userBlogInfoDTO.setCommentTopAvg(new LongWritable(computeTopAvg(commentTopList)));
        userBlogInfoDTO.setRepostTopAvg(new LongWritable(computeTopAvg(repostTopList)));
        multipleOutputs.write(NullWritable.get(), new Text(userBlogInfoDTO.toString()), "userBlogStat/");
        String uid = key.toString();
        if (UserTypeUtils.isCoreUser(uid)) {
            writeTopList(uid, attitudeTopList);
            writeTopList(uid, commentTopList);
            writeTopList(uid, repostTopList);
        }
    }

    private void writeTopList(String uid, List<MidCount> list) throws IOException, InterruptedException {
        for (MidCount midCount : list) {
            String result = uid + "," + midCount.getMid();
            multipleOutputs.write(NullWritable.get(), new Text(result), "uid_mid_top10/");
        }
    }

    @Override
    protected void setup(Context context) throws IOException{
        multipleOutputs = new MultipleOutputs<>(context);
        Configuration conf = context.getConfiguration();
        URI[] uidUris = Job.getInstance(conf).getCacheFiles();
        for (URI uidUri : uidUris) {
            Path uidPath = new Path(uidUri.getPath());
            String uidFileName = uidPath.getName();
            UserTypeUtils.initSet(uidFileName);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }

    private long getMedian(List<Long> list) {
        int length = list.size();
        if (length == 0) {
            return 0L;
        }

        Collections.sort(list);

        if (length % 2 == 0) {
            return (list.get(length / 2) + list.get(length / 2 - 1)) / 2;
        } else {
            return list.get(length / 2);
        }
    }

    private Timestamp transDateTimeStringToTimestamp(String dateTime) {
        if (Pattern.matches(DATE_REGEX, dateTime)) {
            dateTime = dateTime + " 00:00:00";
        }
        return Timestamp.valueOf(dateTime);
    }

    private void addToTopList(List<MidCount> list, MidCount midCount, int size) {
        int length = list.size();
        if (length < size){
            list.add(midCount);
            list.sort(Collections.reverseOrder());
        } else if (list.get(size - 1).compareTo(midCount) < 0){
            list.set(size - 1, midCount);
            list.sort(Collections.reverseOrder());
        }
    }

    private Long computeTopAvg(List<MidCount> list) {
        int size = list.size();
        if (size == 0) {
            return 0L;
        }
        long sum = 0;
        for (MidCount midCount : list){
            sum += midCount.getCount();
        }
        return sum / size;
    }

    class MidCount implements Comparable<MidCount>{
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
            if (this.count > o.count){
                return 1;
            }
            if (this.count < o.count){
                return -1;
            }
            if (this.timestamp.after(o.timestamp)){
                return 1;
            }
            if (this.timestamp.before(o.timestamp)){
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