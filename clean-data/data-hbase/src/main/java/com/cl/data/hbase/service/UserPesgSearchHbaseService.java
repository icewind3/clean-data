package com.cl.data.hbase.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.cl.data.hbase.dto.BlogDTO;
import com.cl.data.hbase.dto.UserBlogListDTO;
import com.cl.data.hbase.util.BuildCsvString;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @author yejianyu
 * @date 2019/8/9
 */
@Slf4j
@Service
public class UserPesgSearchHbaseService {

    @Value(value = "${hbase.table-name.pesg}")
    private String pesgHbaseTableName;

    private static final String BLOG_HBASE_FAMILY_RESULT1 = "result1";
    private static final String DATE_REGEX = "^\\d{4}-\\d{1,2}-\\d{1,2}$";

    private static final String[] PESG_TYPE = {"app", "author", "book", "brand",
        "business_china", "business_group", "business_overseas", "chezaiyongpin", "city", "dianshi", "dongman",
        "duanpian", "ebook", "enterprise", "film", "jilupian", "music", "product", "qichebaoxian",
        "qichechangshang", "qichechexi", "qichechexing", "qichejibie", "qichemeirongchanpin", "qichepeizhi",
        "qichepinpai", "qicheqita", "qichezhengjian", "star_en", "star_zh", "zongyi", "blibli_dongman", "qita_dongman",
        "dongman_people", "yinghua_dongman", "zone", "dongman_biecheng", "dongman_chupingongsi", "dongman_coser",
        "dongman_role", "dongman_shengyou", "dongman_shoubangongsi", "dongman_up", "game", "finance", "travel",
        "chongwu_brand", "chongwu_product", "chongwu_xingwei", "fangdichang_guoneiloupan", "fangdichang_jianzhujiancai",
        "fangdichang_jiazhuangsheji","fangdichang_kaifa", "fangdichang_wuyeguanli", "fangdichang_zhuanyeci",
        "gongyi_jigou", "gongyi_meiti", "gongyi_mingci", "jiaoyu_app_daxue", "jiaoyu_app_kaogong", "jiaoyu_app_waiyu",
        "jiaoyu_app_xuelingqian", "jiaoyu_app_zhongxiaoxue", "jiaoyu_jiaoyuyongpin_brand",
        "jiaoyu_jiaoyuyongpin_product", "jiaoyu_jigou_daxue","jiaoyu_jigou_liuxue", "jiaoyu_jigou_peixun",
        "jiaoyu_jigou_wangxiao", "jiaoyu_jigou_zaojiao","jiaoyu_kaoshi_daxue", "jiaoyu_kaoshi_liuxue",
        "jiaoyu_kaoshi_zhongxiaoxue", "jiaoyu_zixun_wangzhan",  "meiti_daxue", "meiti_dianshi", "meiti_diantai",
        "meiti_jigou", "meiti_mingci", "meiti_star", "sheying_brand_shexiangji", "sheying_brand_shexiangtou",
        "sheying_brand_sheyingqicai", "sheying_brand_xiangji", "sheying_mingci", "sheying_sheyingjia", "yule_fanquan",
        "yule_fensi", "yule_fensi_mingci", "yule_jingjigongsi", "yule_mingci", "yule_xueyuan", "zhichang_app",
        "zhichang_book", "zhichang_mingci", "zhichang_peixun",  "zhichang_shangxueyuan", "zhichang_web",
        "zhichang_zhengshu", "zhichang_zhiwei", "zhichang_zongyi"};

    @Autowired
    private HbaseTemplate hbaseTemplate;

    public String[] getUserBlogPesgResult(Long uid, List<Long> midList) {
        Result result = hbaseTemplate.execute(pesgHbaseTableName, table -> {
            Get get = createUserPesgGet(uid, midList);
            return table.get(get);
        });

        Cell[] cells = result.rawCells();
        Map<String, Map<String, Integer>> groupWordCountMap = new HashMap<>();
        for (Cell cell : cells) {
            String group = Bytes.toString(CellUtil.cloneQualifier(cell));
            String keyword = Bytes.toString(CellUtil.cloneValue(cell));
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

        Map<String, Integer> typeIndexMap = getTypeIndexMap(PESG_TYPE);
        int size = PESG_TYPE.length;
        String[] resultArray = new String[size + 1];
        resultArray[0] = String.valueOf(uid);
        for (int i = 1; i < size; i++) {
            resultArray[i] = "{}";
        }

        for (Map.Entry<String, Map<String, Integer>> entry : groupWordCountMap.entrySet()) {
            String group = entry.getKey();
            Map<String, Integer> value = entry.getValue();
            List<Map.Entry<String, Integer>> list = new LinkedList<>(value.entrySet());
            list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

            Map<String, Integer> resultMap = new LinkedHashMap<>();
            for (Map.Entry<String, Integer> wordCountEntry : list) {
                resultMap.put(wordCountEntry.getKey(), wordCountEntry.getValue());
            }
            resultArray[typeIndexMap.get(group)] = JSON.toJSONString(resultMap, SerializerFeature.UseSingleQuotes);
        }
        return resultArray;
    }

    private Map<String, Integer> getTypeIndexMap(String[] header) {
        int index = 1;
        Map<String, Integer> map = new HashMap<>();
        for (String group : header) {
            map.put(group, index);
            index++;
        }
        return map;
    }

    private Get createUserPesgGet(Long uid, List<Long> midList) {
        Get get = new Get(Bytes.toBytes(String.valueOf(uid)));
        get.setMaxVersions();
        get.setFilter(new TimestampsFilter(midList));
        for (String type : PESG_TYPE) {
            get.addColumn(Bytes.toBytes(BLOG_HBASE_FAMILY_RESULT1), Bytes.toBytes(type));
        }
        return get;
    }

}
