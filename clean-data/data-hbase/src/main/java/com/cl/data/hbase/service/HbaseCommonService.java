package com.cl.data.hbase.service;

import com.cl.data.hbase.dto.UidMidBlogDTO;
import com.cl.data.hbase.dto.UserBlogInfoDTO;
import com.cl.data.hbase.entity.MblogFromUid;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
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
public class HbaseCommonService {

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

    public void deleteOne(String row, long timestamp) {
        hbaseTemplate.execute("uid_mid_blog_test", table -> {
            Delete delete = new Delete(Bytes.toBytes(row));
            delete.addColumn(Bytes.toBytes("weibo"), Bytes.toBytes(BLOG_HBASE_QUALIFIER_ATTITUDES_COUNT), timestamp);
            delete.addColumn(Bytes.toBytes("weibo"), Bytes.toBytes(BLOG_HBASE_QUALIFIER_COMMENTS_COUNT), timestamp);
            delete.addColumn(Bytes.toBytes("weibo"), Bytes.toBytes(BLOG_HBASE_QUALIFIER_REPOSTS_COUNT), timestamp);
            delete.addColumn(Bytes.toBytes("weibo"), Bytes.toBytes(BLOG_HBASE_QUALIFIER_CREATED_AT), timestamp);
            delete.addColumn(Bytes.toBytes("weibo"), Bytes.toBytes(BLOG_HBASE_QUALIFIER_MID), timestamp);
            delete.addColumn(Bytes.toBytes("weibo"), Bytes.toBytes(BLOG_HBASE_QUALIFIER_BID), timestamp);
            delete.addColumn(Bytes.toBytes("weibo"), Bytes.toBytes(BLOG_HBASE_QUALIFIER_IS_RETWEETED), timestamp);
            delete.addColumn(Bytes.toBytes("weibo"), Bytes.toBytes(BLOG_HBASE_QUALIFIER_TEXT), timestamp);
            delete.addColumn(Bytes.toBytes("weibo"), Bytes.toBytes(BLOG_HBASE_QUALIFIER_RETWEETED_TEXT), timestamp);
            table.delete(delete);
            return null;
        });
    }

    public void deleteRow(String row, long timestamp) {
        hbaseTemplate.execute("uid_mid_blog_test", table -> {
            Delete delete = new Delete(Bytes.toBytes(row), timestamp);
            table.delete(delete);
            return null;
        });
    }



}
