package com.cl.data.stat.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cl.graph.weibo.core.util.CsvFileHelper;
import com.cl.graph.weibo.core.util.CsvFileUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author yejianyu
 * @date 2019/9/19
 */
public class FileUtil {
    public static Set<Long> readColumn(String filepath, int column) throws IOException {
        CSVParser csvParser = CSVParser.parse(new File(filepath), StandardCharsets.UTF_8, CSVFormat.DEFAULT);
        List<CSVRecord> recordList = csvParser.getRecords();
        Set<Long> set = new HashSet<>();
        for(CSVRecord csvRecord : recordList) {
            set.add(Long.valueOf(csvRecord.get(column)));
        }

        csvParser.close();
        return set;
    }
}
