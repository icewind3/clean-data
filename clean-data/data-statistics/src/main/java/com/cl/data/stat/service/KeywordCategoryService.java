package com.cl.data.stat.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cl.graph.weibo.core.util.CsvFileHelper;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author yejianyu
 * @date 2019/10/25
 */
@Service
public class KeywordCategoryService {

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

    public void computeCategory(String deleteWordFile, String categoryFile, String filePath, String resultPath) {
        try {
            Set<String> deleteSet = getDeleteSet(deleteWordFile);
            Map<String, Set<String>> categoryMap = new HashMap<>();
            try (CSVParser csvParser = CsvFileHelper.reader(categoryFile)) {
                for (CSVRecord record : csvParser) {
                    String word  = record.get(0);
                    String category  = record.get(1).trim();
                    if (categoryMap.containsKey(word)){
                        Set<String> set = categoryMap.get(word);
                        set.add(category);
                    } else {
                        Set<String> set = new HashSet<>();
                        set.add(category);
                        categoryMap.put(word, set);
                    }
                }
            }

            Map<String, Set<Long>> categoryCountMap = new HashMap<>();
            try (CSVParser csvParser = CsvFileHelper.reader(filePath, HEADER_RESULT)) {
                for (CSVRecord record : csvParser) {
                    Long uid = Long.parseLong(record.get(0));
                    JSONObject jsonObject = JSON.parseObject(record.get(1));
                    jsonObject.forEach((word, count) -> {
                        if (deleteSet.contains(word)){
                            return;
                        }
                        Set<String> set = categoryMap.get(word);
                        if (set == null || set.size() == 0) {
                            String category = "其它";
                            if (categoryCountMap.containsKey(category)){
                                Set<Long> uidSet = categoryCountMap.get(category);
                                uidSet.add(uid);
                            } else {
                                Set<Long> uidSet = new HashSet<>();
                                uidSet.add(uid);
                                categoryCountMap.put(category, uidSet);
                            }
                            return;
                        }
                        for (String category : set) {
                            if (categoryCountMap.containsKey(category)){
                                Set<Long> uidSet = categoryCountMap.get(category);
                                uidSet.add(uid);
                            } else {
                                Set<Long> uidSet = new HashSet<>();
                                uidSet.add(uid);
                                categoryCountMap.put(category, uidSet);
                            }
                        }
                    });
                }
            }
            try (CSVPrinter csvPrinter = CsvFileHelper.writer(resultPath)){
                categoryCountMap.forEach((category, uidSet) -> {
                    try {
                        csvPrinter.printRecord(category, uidSet.size());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private Set<String> getDeleteSet(String filePath) throws IOException {
        Set<String> set = new HashSet<>();
        if (StringUtils.isBlank(filePath)) {
            return set;
        }
        BufferedReader fis = new BufferedReader(new FileReader(filePath));
        String word;
        while ((word = fis.readLine()) != null) {
            set.add(word.trim());
        }
        return set;
    }

    public void filterType(String filePath, String resultPath) {
        try (CSVParser csvParser = CsvFileHelper.reader(filePath, HEADER_RESULT);
//             CSVPrinter dianshiWriter = CsvFileHelper.writer(resultPath + File.separator + "user_pesg_stat_dianshi.csv");
//             CSVPrinter dongmanWriter = CsvFileHelper.writer(resultPath + File.separator + "user_pesg_stat_dongman.csv");
//             CSVPrinter duanpianWriter = CsvFileHelper.writer(resultPath + File.separator + "user_pesg_stat_duanpian.csv");
             CSVPrinter zoneWriter = CsvFileHelper.writer(resultPath + File.separator + "user_pesg_stat_zone.csv");
             CSVPrinter zhongyiWriter = CsvFileHelper.writer(resultPath + File.separator + "user_pesg_stat_zhongyi.csv")) {
            for (CSVRecord record : csvParser) {
                String uid = record.get(0);
//                write(dianshiWriter, uid, record.get("dianshi"));
//                write(dongmanWriter, uid, record.get("dongman"));
//                write(duanpianWriter, uid, record.get("duanpian"));
                write(zoneWriter, uid, record.get("zone"));
                write(zhongyiWriter, uid, record.get("zongyi"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void write(CSVPrinter csvPrinter, String uid, String value) throws IOException {
        if ("{}".equals(value)) {
            return;
        }
        csvPrinter.printRecord(uid, value);
    }
}
