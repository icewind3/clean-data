package com.cl.data.stat.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cl.graph.weibo.core.constant.FileSuffixConstants;
import com.cl.graph.weibo.core.util.CsvFileHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

/**
 * @author yejianyu
 * @date 2019/9/6
 */
@Service
@Slf4j
public class KeywordAllStatisticsService {

//    private static final String[] HEADER_RESULT = {"uid", "app", "author", "book", "brand",
//        "business_china", "business_group", "business_overseas", "chezaiyongpin", "city", "dianshi", "dongman",
//        "duanpian", "ebook", "enterprise", "film", "jilupian", "music", "product", "qichebaoxian",
//        "qichechangshang", "qichechexi", "qichechexing", "qichejibie", "qichemeirongchanpin", "qichepeizhi",
//        "qichepinpai", "qicheqita", "qichezhengjian", "star_en", "star_zh", "zongyi", "blibli_dongman", "qita_dongman",
//        "dongman_people", "yinghua_dongman", "zone", "dongman_biecheng", "dongman_chupingongsi", "dongman_coser",
//        "dongman_role", "dongman_shengyou", "dongman_shoubangongsi", "dongman_up", "game"};

    private static final String[] HEADER_RESULT = {"uid", "app", "author", "book", "brand",
        "business_china", "business_group", "business_overseas", "chezaiyongpin", "city", "dianshi", "dongman",
        "duanpian", "ebook", "enterprise", "film", "jilupian", "music", "product", "qichebaoxian",
        "qichechangshang", "qichechexi", "qichechexing", "qichejibie", "qichemeirongchanpin", "qichepeizhi",
        "qichepinpai", "qicheqita", "qichezhengjian", "star_en", "star_zh", "zongyi", "blibli_dongman", "qita_dongman",
        "dongman_people", "yinghua_dongman", "zone", "dongman_biecheng", "dongman_chupingongsi", "dongman_coser",
        "dongman_role", "dongman_shengyou", "dongman_shoubangongsi", "dongman_up", "game", "finance", "travel",
        "chongwu_brand", "chongwu_product", "chongwu_xingwei", "fangdichang_guoneiloupan", "fangdichang_jianzhujiancai",
        "fangdichang_jiazhuangsheji", "fangdichang_kaifa", "fangdichang_wuyeguanli", "fangdichang_zhuanyeci",
        "gongyi_jigou", "gongyi_meiti", "gongyi_mingci", "jiaoyu_app_daxue", "jiaoyu_app_kaogong", "jiaoyu_app_waiyu",
        "jiaoyu_app_xuelingqian", "jiaoyu_app_zhongxiaoxue", "jiaoyu_jiaoyuyongpin_brand",
        "jiaoyu_jiaoyuyongpin_product", "jiaoyu_jigou_daxue", "jiaoyu_jigou_liuxue", "jiaoyu_jigou_peixun",
        "jiaoyu_jigou_wangxiao", "jiaoyu_jigou_zaojiao", "jiaoyu_kaoshi_daxue", "jiaoyu_kaoshi_liuxue",
        "jiaoyu_kaoshi_zhongxiaoxue", "jiaoyu_zixun_wangzhan", "meiti_daxue", "meiti_dianshi", "meiti_diantai",
        "meiti_jigou", "meiti_mingci", "meiti_star", "sheying_brand_shexiangji", "sheying_brand_shexiangtou",
        "sheying_brand_sheyingqicai", "sheying_brand_xiangji", "sheying_mingci", "sheying_sheyingjia", "yule_fanquan",
        "yule_fensi", "yule_fensi_mingci", "yule_jingjigongsi", "yule_mingci", "yule_xueyuan", "zhichang_app",
        "zhichang_book", "zhichang_mingci", "zhichang_peixun", "zhichang_shangxueyuan", "zhichang_web",
        "zhichang_zhengshu", "zhichang_zhiwei", "zhichang_zongyi"};

    public void countByPeopleStat(String filePath, String resultPath) {
        File file = new File(resultPath);
        file.mkdirs();
        int uidCount = 0;
        Map<String, Map<String, Integer>> groupWordCountMap = new HashMap<>();
        try (CSVParser csvParser = CsvFileHelper.reader(filePath, HEADER_RESULT)) {
            for (CSVRecord record : csvParser) {
                uidCount++;
                for (String group : HEADER_RESULT) {
                    if ("uid".equals(group)) {
                        continue;
                    }
                    String keyword = record.get(group);
                    Map<String, Integer> wordCountMap;
                    if (groupWordCountMap.containsKey(group)) {
                        wordCountMap = groupWordCountMap.get(group);
                    } else {
                        wordCountMap = new HashMap<>();
                        groupWordCountMap.put(group, wordCountMap);
                    }
                    JSONObject jsonObject = JSON.parseObject(keyword);
                    jsonObject.forEach((word, count) -> {
                        wordCountMap.merge(word, 1, Integer::sum);
                    });
                }
                if (uidCount % 50000 == 0){
                    log.info("已完成人数为: " + uidCount);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        groupWordCountMap.forEach((group, wordCountMap) -> {
            try (CSVPrinter printer = CsvFileHelper.writer(resultPath + File.separator + group
                + FileSuffixConstants.CSV)) {
                List<Map.Entry<String, Integer>> list = new ArrayList<>(wordCountMap.entrySet());
                list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));
                for (Map.Entry<String, Integer> entry : list) {
                    printer.printRecord(entry.getKey(), entry.getValue());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        log.info("总人数为: " + uidCount);
    }

    public void countByWordFrequency(String filePath, String resultPath) {
        File file = new File(resultPath);
        file.mkdirs();
        int uidCount = 0;
        Map<String, Map<String, Integer>> groupWordCountMap = new HashMap<>();
        try (CSVParser csvParser = CsvFileHelper.reader(filePath, HEADER_RESULT)) {
            for (CSVRecord record : csvParser) {
                uidCount++;
                for (String group : HEADER_RESULT) {
                    if ("uid".equals(group)) {
                        continue;
                    }
                    String keyword = record.get(group);
                    Map<String, Integer> wordCountMap;
                    if (groupWordCountMap.containsKey(group)) {
                        wordCountMap = groupWordCountMap.get(group);
                    } else {
                        wordCountMap = new HashMap<>();
                        groupWordCountMap.put(group, wordCountMap);
                    }
                    JSONObject jsonObject = JSON.parseObject(keyword);
                    jsonObject.forEach((word, count) -> {
                        wordCountMap.merge(word, (Integer) count, Integer::sum);
                    });
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        groupWordCountMap.forEach((group, wordCountMap) -> {
            try (CSVPrinter printer = CsvFileHelper.writer(resultPath + File.separator + "all" + File.separator
                + group + FileSuffixConstants.CSV);
                 CSVPrinter printer2 = CsvFileHelper.writer(resultPath + File.separator + "top100" + File.separator
                     + group + "_top100_" + FileSuffixConstants.CSV)) {
                List<Map.Entry<String, Integer>> list = new ArrayList<>(wordCountMap.entrySet());

                list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));
                int count = 0;
                for (Map.Entry<String, Integer> entry : list) {
                    count++;
                    printer.printRecord(entry.getKey(), entry.getValue());
                    if (count <= 100) {
                        printer2.printRecord(entry.getKey(), entry.getValue());
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        log.info("总人数为: " + uidCount);
    }
}
