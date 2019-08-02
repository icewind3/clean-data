package com.cl.graph.weibo.data.strategy;

import com.cl.graph.weibo.data.manager.CsvFile;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author yejianyu
 * @date 2019/7/26
 */
public interface DataToFileStrategy<RAW, RESULT> {

    void wrapDataToMap(List<RAW> list, Map<String, List<RESULT>> map);

    void writeToCsvFile(List<RESULT> list, String filePath) throws IOException;
}
