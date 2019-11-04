package com.cl.data.hbase.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/9/3
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class WordSegmentationHbaseServiceTest {

    @Autowired
    private HbaseTemplate hbaseTemplate;

    @Value(value = "${hbase.table-name.pesg}")
    private String pesgHbaseTableName;

    @Test
    public void get(){
        String uid = "1002410351";
        Result result = hbaseTemplate.execute(pesgHbaseTableName, table -> {
            Get get = new Get(Bytes.toBytes(uid));
            get.setMaxVersions();
            get.addColumn(Bytes.toBytes("result3"), Bytes.toBytes("product_caizhi"));
            return table.get(get);
        });
        System.out.println(result);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            String key = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println("key=" + key + ",value=" + value);
        }
    }

    @Test
    public void put(){
        hbaseTemplate.execute(pesgHbaseTableName, table -> {
            Put put = createPut();
            table.put(put);
            return null;
        });
    }

    private Put createPut() {
        String column = "result1";
        String rowKey = "5446631898";
        String mid = "4376659291851923";

        long ts = Long.parseLong(mid);
        Put put = new Put(Bytes.toBytes(rowKey));
        addColumn(put, column, "ebook", ts, "{}", "{}");
        addColumn(put, column, "brand", ts, "{'甘露': 1}", "{}");
        addColumn(put, column, "created_at", ts, "2019-05-27 00:00:00", StringUtils.EMPTY);
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