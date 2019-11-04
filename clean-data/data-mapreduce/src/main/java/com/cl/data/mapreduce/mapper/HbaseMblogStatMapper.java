package com.cl.data.mapreduce.mapper;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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
import java.time.LocalDateTime;
import java.util.regex.Pattern;

/**
 * @author yejianyu
 * @date 2019/10/23
 */
@Slf4j
public class HbaseMblogStatMapper extends TableMapper<Text, NullWritable> {

    private static final String BLOG_HBASE_QUALIFIER_CREATED_AT = "created_at";
    private static final String BLOG_HBASE_QUALIFIER_IS_RETWEETED = "is_retweeted";
    private static final String DATE_REGEX = "^\\d{4}-\\d{1,2}-\\d{1,2}$";
    private static final int ONE_WEEK_MILLIS = 1000 * 60 * 60 * 24 * 7;

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        String uid = Bytes.toString(key.get());
        if (!StringUtils.isNumeric(uid)) {
            return;
        }
        Cell[] cells = value.rawCells();
        Timestamp releaseMblogEarly = Timestamp.valueOf(LocalDateTime.now());
        Timestamp releaseMblogLately = new Timestamp(0L);
        int retweetedCount = 0;
        int blogCount = 0;
        for (Cell cell : cells) {
            long mid = cell.getTimestamp();
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String cValue = Bytes.toString(CellUtil.cloneValue(cell));
            try {
                switch (qualifier) {
                    case BLOG_HBASE_QUALIFIER_CREATED_AT:
                        try {
                            Timestamp timestamp = transDateTimeStringToTimestamp(cValue);
                            if (timestamp.before(releaseMblogEarly)) {
                                releaseMblogEarly = timestamp;
                            }
                            if (timestamp.after(releaseMblogLately)) {
                                releaseMblogLately = timestamp;
                            }
                        } catch (IllegalArgumentException e) {
                            log.error("时间格式转换错误：uid={}, mid={}, dateTime={}", uid, cell.getTimestamp(), cValue);
                        }
                        break;
                    case BLOG_HBASE_QUALIFIER_IS_RETWEETED:
                        if ("1".equals(cValue)) {
                            retweetedCount++;
                        }
                        blogCount++;
                        break;
                    default:
                        break;
                }
            } catch (NumberFormatException e) {
                String errorMsg = String.format("数字转换错误, uid=%s, mid=%d, key=%s,value=%s", uid, mid, key, value);
                log.error(errorMsg, e);
            }
        }
        if (blogCount <= 0){
            return;
        }
        int originalCount = blogCount - retweetedCount;
        float ratio = (float) originalCount / blogCount;
        Timestamp recentTime = Timestamp.valueOf(LocalDateTime.now());
        long week = (recentTime.getTime() - releaseMblogEarly.getTime()) / (ONE_WEEK_MILLIS);
        week = week == 0 ? 1 : week;
        float releaseMblogFrequency = (float) blogCount / week;
        float releaseOriginalBlogFrequency = (float) originalCount / week;
        String s = String.format("%s,%d,%d,%s,%s,%f,%f,%f",uid, blogCount, retweetedCount, releaseMblogEarly,
            releaseMblogLately, releaseMblogFrequency, releaseOriginalBlogFrequency, ratio);
        context.write(new Text(s), NullWritable.get());
    }

    private static Timestamp transDateTimeStringToTimestamp(String dateTime) {
        if (Pattern.matches(DATE_REGEX, dateTime)) {
            dateTime = dateTime + " 00:00:00";
        }
        return Timestamp.valueOf(dateTime);
    }
}
