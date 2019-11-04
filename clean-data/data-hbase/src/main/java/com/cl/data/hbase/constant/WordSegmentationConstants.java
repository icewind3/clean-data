package com.cl.data.hbase.constant;

/**
 * @author yejianyu
 * @date 2019/9/4
 */
public class WordSegmentationConstants {

    public static final String[] HEADER_RESULT_1 = {"uid", "mid", "ebook", "enterprise", "music", "film", "city",
        "product", "brand", "star", "app", "created_at", "weight"};

    public static final String[] HEADER_RESULT_2 = {"uid", "mid", "aroma_type", "brand", "function", "oil", "product",
        "proper_nouns", "scene", "created_at", "weight"};

    public static final String[] HEADER_RESULT_3 = {"uid", "mid", "brand", "caizhi", "function", "mingci", "oil",
        "pingjia", "product", "product_caizhi", "scene", "tiyangan", "xiangguan_function", "xiangxing",
        "xingrong", "created_at", "weight"};

    public static final String[] HEADER_RESULT_NEW_KOL = {"uid", "mid", "app", "author", "book", "brand",
        "business_china", "business_group", "business_overseas", "chezaiyongpin", "city", "dianshi", "dongman",
        "duanpian", "ebook", "enterprise", "film", "jilupian", "music", "product", "qichebaoxian",
        "qichechangshang", "qichechexi", "qichechexing", "qichejibie", "qichemeirongchanpin", "qichepeizhi",
        "qichepinpai", "qicheqita", "qichezhengjian", "star_en", "star_zh", "zongyi", "blibli_dongman", "qita_dongman",
        "dongman_people", "yinghua_dongman", "zone", "dongman_biecheng", "dongman_chupingongsi", "dongman_coser",
        "dongman_role", "dongman_shengyou", "dongman_shoubangongsi", "dongman_up", "game", "created_at", "weight"};

    public static final String[] HEADER_RESULT_FANS = {"uid", "mid", "app", "author", "book", "brand",
        "business_china", "business_group", "business_overseas", "chezaiyongpin", "city", "dianshi", "dongman",
        "duanpian", "ebook", "enterprise", "film", "jilupian", "music", "product", "qichebaoxian",
        "qichechangshang", "qichechexi", "qichechexing", "qichejibie", "qichemeirongchanpin", "qichepeizhi",
        "qichepinpai", "qicheqita", "qichezhengjian", "star_en", "star_zh", "zongyi", "blibli_dongman", "qita_dongman",
        "dongman_people", "yinghua_dongman", "zone", "dongman_biecheng", "dongman_chupingongsi", "dongman_coser",
        "dongman_role", "dongman_shengyou", "dongman_shoubangongsi", "dongman_up", "game", "finance", "travel",
        "created_at", "weight"};

    public static final String FAMILY_RESULT_1 = "result1";
    public static final String FAMILY_RESULT_2 = "result2";
    public static final String FAMILY_RESULT_3 = "result3";
}
