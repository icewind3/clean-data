package com.cl.data.mapreduce.constant;

/**
 * @author yejianyu
 * @date 2019/9/4
 */
public class WordSegmentationConstants {

    public static final String[] HEADER_RESULT_1 = {"uid", "mid", "app", "author", "book", "brand",
        "business_china", "business_group", "business_overseas", "chezaiyongpin", "city", "dianshi", "dongman",
        "duanpian", "ebook", "enterprise", "film", "jilupian", "music", "product", "qichebaoxian",
        "qichechangshang", "qichechexi", "qichechexing", "qichejibie", "qichemeirongchanpin", "qichepeizhi",
        "qichepinpai", "qicheqita", "qichezhengjian", "star_en", "star_zh", "zongyi", "created_at", "weight"};

    public static final String[] HEADER_RESULT_2 = {"uid", "mid", "blibli_dongman", "qita_dongman",
        "dongman_people", "yinghua_dongman", "zone", "created_at", "weight"};

    public static final String[] HEADER_RESULT_3 = {"uid", "mid", "dongman_biecheng", "dongman_chupingongsi",
        "dongman_coser", "dongman_role", "dongman_shengyou", "dongman_shoubangongsi", "dongman_up", "finance", "game",
        "travel", "created_at", "weight"};

    public static final String[] HEADER_RESULT_LABEL_GRAPH = {"uid", "mid", "brand", "city", "ebook", "music",
        "product", "star_en", "star_zh", "zone", "created_at", "weight"};

    public static final String[] HEADER_RESULT_NEW_KOL = {"uid", "mid", "app", "author", "book", "brand",
        "business_china", "business_group", "business_overseas", "chezaiyongpin", "city", "dianshi", "dongman",
        "duanpian", "ebook", "enterprise", "film", "jilupian", "music", "product", "qichebaoxian",
        "qichechangshang", "qichechexi", "qichechexing", "qichejibie", "qichemeirongchanpin", "qichepeizhi",
        "qichepinpai", "qicheqita", "qichezhengjian", "star_en", "star_zh", "zongyi", "blibli_dongman", "qita_dongman",
        "dongman_people", "yinghua_dongman", "zone", "dongman_biecheng", "dongman_chupingongsi", "dongman_coser",
        "dongman_role", "dongman_shengyou", "dongman_shoubangongsi", "dongman_up", "game", "created_at", "weight"};

    public static final String[] HEADER_RESULT_NEW_KOL_2 = {"uid", "mid", "finance", "travel", "created_at", "weight"};

    public static final String[] HEADER_RESULT_80W_KOL_1 = {"uid", "mid", "chongwu_brand", "chongwu_product",
        "chongwu_xingwei", "fangdichang_guoneiloupan", "fangdichang_jianzhujiancai", "fangdichang_jiazhuangsheji",
        "fangdichang_kaifa", "fangdichang_wuyeguanli", "fangdichang_zhuanyeci", "gongyi_jigou", "gongyi_meiti",
        "gongyi_mingci", "jiaoyu_app_daxue", "jiaoyu_app_kaogong", "jiaoyu_app_waiyu", "jiaoyu_app_xuelingqian",
        "jiaoyu_app_zhongxiaoxue", "jiaoyu_jiaoyuyongpin_brand", "jiaoyu_jiaoyuyongpin_product", "jiaoyu_jigou_daxue",
        "jiaoyu_jigou_liuxue", "jiaoyu_jigou_peixun", "jiaoyu_jigou_wangxiao", "jiaoyu_jigou_zaojiao",
        "jiaoyu_kaoshi_daxue", "jiaoyu_kaoshi_liuxue", "jiaoyu_kaoshi_zhongxiaoxue", "jiaoyu_zixun_wangzhan",
        "meiti_daxue", "meiti_dianshi", "meiti_diantai", "meiti_jigou", "meiti_mingci", "meiti_star",
        "sheying_brand_shexiangji", "sheying_brand_shexiangtou", "sheying_brand_sheyingqicai", "sheying_brand_xiangji",
        "sheying_mingci", "sheying_sheyingjia", "yule_fanquan", "yule_fensi", "yule_fensi_mingci", "yule_jingjigongsi",
        "yule_mingci", "yule_xueyuan", "zhichang_app", "zhichang_book", "zhichang_mingci", "zhichang_peixun",
        "zhichang_shangxueyuan", "zhichang_web", "zhichang_zhengshu", "zhichang_zhiwei", "zhichang_zongyi",
        "created_at", "weight"};

    public static final String[] HEADER_RESULT_MERGE = {"uid", "mid", "app", "author", "book", "brand",
        "business_china", "business_group", "business_overseas", "chezaiyongpin", "city", "dianshi", "dongman",
        "duanpian", "ebook", "enterprise", "film", "jilupian", "music", "product", "qichebaoxian",
        "qichechangshang", "qichechexi", "qichechexing", "qichejibie", "qichemeirongchanpin", "qichepeizhi",
        "qichepinpai", "qicheqita", "qichezhengjian", "star_en", "star_zh", "zongyi", "blibli_dongman", "qita_dongman",
        "dongman_people", "yinghua_dongman", "zone", "dongman_biecheng", "dongman_chupingongsi", "dongman_coser",
        "dongman_role", "dongman_shengyou", "dongman_shoubangongsi", "dongman_up", "game", "finance", "travel",
        "created_at", "weight"};

    public static final String[] HEADER_RESULT_FANS = {"uid", "mid", "app", "author", "book", "brand",
        "business_china", "business_group", "business_overseas", "chezaiyongpin", "city", "dianshi", "dongman",
        "duanpian", "ebook", "enterprise", "film", "jilupian", "music", "product", "qichebaoxian",
        "qichechangshang", "qichechexi", "qichechexing", "qichejibie", "qichemeirongchanpin", "qichepeizhi",
        "qichepinpai", "qicheqita", "qichezhengjian", "star_en", "star_zh", "zongyi", "blibli_dongman", "qita_dongman",
        "dongman_people", "yinghua_dongman", "zone", "dongman_biecheng", "dongman_chupingongsi", "dongman_coser",
        "dongman_role", "dongman_shengyou", "dongman_shoubangongsi", "dongman_up", "game", "finance", "travel",
        "created_at", "weight"};

    public static final String[] HEADER_RESULT_KOL_ZONE = {"uid", "mid", "财经", "电影", "动漫", "读书",
        "法律", "房地产", "工农贸易", "公益", "国画", "国学", "海外", "航空", "互联网", "婚庆服务", "机构场所",
        "家居", "健康养生", "健康医疗", "教育", "军事", "科学科普", "历史", "旅游", "媒体", "美食", "美术", "萌宠",
        "母婴", "汽车", "情感", "区域号", "人文艺术", "日用百货", "三农", "商务服务", "设计", "社会团体", "摄影",
        "生活服务", "时尚美妆", "收藏", "书法", "数码", "体育", "武术", "舞蹈", "校园", "星座", "音乐", "游戏", "娱乐",
        "运动健身", "政府", "职场", "宗教", "综艺", "created_at", "weight"};

    public static final String[] HEADER_RESULT_FANS_SECOND = {"uid", "mid", "app", "author", "book", "brand",
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
        "zhichang_zhengshu", "zhichang_zhiwei", "zhichang_zongyi", "created_at", "weight"};

    public static final String FAMILY_RESULT_1 = "result1";
    public static final String FAMILY_RESULT_2 = "result2";
    public static final String FAMILY_RESULT_3 = "result3";

    public static final String TYPE_RESULT_1 = "result1";
    public static final String TYPE_RESULT_2 = "result2";
    public static final String TYPE_RESULT_3 = "result3";
    public static final String TYPE_NEW_KOL = "newKOL";
    public static final String TYPE_NEW_KOL_2 = "newKOL_2";
    public static final String TYPE_80W_KOL = "80w_kol";
    public static final String TYPE_FANS = "fans_result";
    public static final String TYPE_FANS_SECOND = "fans_second_result";
    public static final String TYPE_KOL_ZONE = "kol_zone";

    public static void main(String[] args) {
        System.out.println(HEADER_RESULT_KOL_ZONE.length);
    }
}
