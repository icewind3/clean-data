package com.cl.data.hbase.service;

import com.cl.data.hbase.dto.BlogAnalysisResultDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * @author yejianyu
 * @date 2019/8/9
 */
@Slf4j
@Service
public class WordSegmentationHbaseService {

    @Value(value = "${hbase.table-name.pesg}")
    private String pesgHbaseTableName;

    private static final String WORD_DEFAULT_VALUE = "{}";

    @Autowired
    private HbaseTemplate hbaseTemplate;

    public void insertBlogAnalysisResult(String family, BlogAnalysisResultDTO blogAnalysisResult) {
        hbaseTemplate.execute(pesgHbaseTableName, table -> {
            Put put = createPut(family, blogAnalysisResult);
            table.put(put);
            return null;
        });
    }

    public void batchInsertBlogAnalysisResult(String family, List<BlogAnalysisResultDTO> blogAnalysisResultList) {
        hbaseTemplate.execute(pesgHbaseTableName, table -> {
            List<Put> list = new ArrayList<>();
            blogAnalysisResultList.forEach(blogAnalysisResult -> {
                Put put = createPut(family, blogAnalysisResult);
                list.add(put);
            });
            table.put(list);
            return null;
        });
    }

    private Put createPut(String family, BlogAnalysisResultDTO blogAnalysisResult) {

        String rowKey = blogAnalysisResult.getUid();
        String mid = blogAnalysisResult.getMid();

        if (StringUtils.isBlank(rowKey) || StringUtils.isBlank(mid)){
            throw new RuntimeException("uid或mid不能为空");
        }

        long ts = Long.parseLong(mid);
        Put put = new Put(Bytes.toBytes(rowKey));
        Map<String, String> typeWordMap = blogAnalysisResult.getTypeWordMap();
        typeWordMap.forEach((key, value) -> {
            addColumn(put, family, key, ts, value, WORD_DEFAULT_VALUE);
        });
//        String createdAt = blogAnalysisResult.getCreatedAt();
//        if (createdAt != null) {
//            addColumn(put, family, "created_at", ts, blogAnalysisResult.getCreatedAt(), StringUtils.EMPTY);
//        }
//        addColumn(put, family, "mid", ts, mid, StringUtils.EMPTY);
        return put;
    }

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
