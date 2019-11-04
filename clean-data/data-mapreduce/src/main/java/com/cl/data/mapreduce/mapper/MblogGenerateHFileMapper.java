package com.cl.data.mapreduce.mapper;

import com.cl.data.mapreduce.constant.WordSegmentationConstants;
import com.cl.data.mapreduce.util.DateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * @author yejianyu
 * @date 2019/9/27
 */
@Slf4j
public class MblogGenerateHFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    private static final String[] HEADER = new String[]{"mid", "uid", "reposts_count", "comments_count",
        "attitudes_count", "text", "retweeted_text", "created_at", "bid", "is_retweeted"};

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

    private static final DateTimeFormatter DATE_TIME_FORMATTER_DISPLAY =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String[] header = HEADER;
        CSVParser parse = CSVParser.parse(value.toString(), CSVFormat.DEFAULT.withHeader(header));
        for (CSVRecord record : parse) {
            if (record.size() != header.length) {
                continue;
            }
            String uid = record.get("uid");
            String mid = record.get("mid");
            if (!StringUtils.isNumeric(uid) || !StringUtils.isNumeric(uid)) {
                log.error("uid_mid_blog data error, uid={}, mid={}, value={}", uid, mid, value.toString());
                continue;
            }

            String repostsCount = record.get("reposts_count");
            String commentsCount = record.get("comments_count");
            String attitudesCount = record.get("attitudes_count");
            if (!StringUtils.isNumeric(repostsCount) || !StringUtils.isNumeric(commentsCount)
                || !StringUtils.isNumeric(attitudesCount)) {
                log.error("uid_mid_blog count data error, uid={}, mid={}, value={}", uid, mid, value.toString());
                continue;
            }

            String column = BLOG_HBASE_FAMILY_WEIBO;

            //拼装rowkey和put
            ImmutableBytesWritable putRowKey = new ImmutableBytesWritable(uid.getBytes());
            long ts = Long.parseLong(mid);
            Put put = new Put(Bytes.toBytes(uid));

            String createdAt = record.get("created_at");
            if (Pattern.matches(DATE_REGEX, createdAt)) {
                createdAt = createdAt + " 00:00:00";
            }
            String createdTime = DATE_TIME_FORMATTER_DISPLAY.format(LocalDateTime.parse(createdAt));

            addColumn(put, column, BLOG_HBASE_QUALIFIER_REPOSTS_COUNT, ts, repostsCount, "0");
            addColumn(put, column, BLOG_HBASE_QUALIFIER_COMMENTS_COUNT, ts, commentsCount, "0");
            addColumn(put, column, BLOG_HBASE_QUALIFIER_ATTITUDES_COUNT, ts, attitudesCount, "0");
            addColumn(put, column, BLOG_HBASE_QUALIFIER_CREATED_AT, ts, createdTime, StringUtils.EMPTY);
            addColumn(put, column, BLOG_HBASE_QUALIFIER_MID, ts, mid, StringUtils.EMPTY);
            addColumn(put, column, BLOG_HBASE_QUALIFIER_BID, ts, record.get("bid"), StringUtils.EMPTY);
            addColumn(put, column, BLOG_HBASE_QUALIFIER_TEXT, ts, record.get("text"), StringUtils.EMPTY);
            addColumn(put, column, BLOG_HBASE_QUALIFIER_RETWEETED_TEXT, ts, record.get("retweeted_text"), StringUtils.EMPTY);
            addColumn(put, column, BLOG_HBASE_QUALIFIER_IS_RETWEETED, ts, record.get("is_retweeted"), "0");
            context.write(putRowKey, put);
        }
    }

    public static void main(String[] args) {
        String createdAt = "2019-09-12";
        if (Pattern.matches(DATE_REGEX, createdAt)) {
            createdAt = createdAt + " 00:00:00";
        }
        String createdTime = DATE_TIME_FORMATTER_DISPLAY.format(LocalDateTime.parse(createdAt));
        System.out.println(createdTime);
    }


//    @Override
//    protected void map(LongWritable key, Text value, Context context)
//        throws IOException, InterruptedException {
//        String[] header = {"uid", "mid", "is_retweeted"};
//        CSVParser parse = CSVParser.parse(value.toString(), CSVFormat.DEFAULT.withHeader(header));
//        for (CSVRecord record : parse) {
//            if (record.size() != header.length) {
//                continue;
//            }
//            String uid = record.get(0);
//            String mid = record.get(1);
//            if (StringUtils.isBlank(uid) || StringUtils.isBlank(mid)) {
//                continue;
//            }
//            try {
//                Long.parseLong(uid);
//                Long.parseLong(mid);
//            } catch (Exception e) {
//                log.error("uid_mid_is_retweeted data error, uid={}, mid={}, value={}", uid, mid, value.toString());
//                continue;
//            }
//
//            String columnFamily = "weibo";
//
//            //拼装rowkey和put
//            ImmutableBytesWritable putRowKey = new ImmutableBytesWritable(uid.getBytes());
//            long ts = Long.parseLong(mid);
//            Put put = new Put(Bytes.toBytes(uid));
//            addColumn(put, columnFamily, "is_retweeted", ts, record.get(2), "0");
//            context.write(putRowKey, put);
//        }
//    }

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
