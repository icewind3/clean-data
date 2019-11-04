package com.cl.data.stat.dictionary;

import org.apache.commons.lang3.StringUtils;

import static com.cl.data.stat.constant.WeiboUserTypeConstants.*;

/**
 * @author Ye Jianyu
 * @date 2019-08-04
 */
public final class Dictionary {

    private static final String GENDER_CODE_MALE = "m";
    private static final String GENDER_CODE_FEMALE = "f";

    public static String getGenderName(String gender) {
        if (StringUtils.equals(GENDER_CODE_MALE, gender)) {
            return "男";
        } else if (StringUtils.equals(GENDER_CODE_FEMALE, gender)) {
            return "女";
        } else {
            return "其他";
        }
    }

    public static String getVerifiedTypeName(Integer verifiedType, Integer verifiedTypeExt) {
        if (verifiedType == null) {
            return "其他";
        }
        if (verifiedType == 0) {
            if (verifiedTypeExt == 0) {
                return "黄V";
            }
            if (verifiedTypeExt == 1) {
                return "红V";
            }
        }
        String verifiedTypeName;
        switch (verifiedType) {
            case PERSONAL:
                verifiedTypeName = "普通用户";
                break;
            case BLUE_V_GOVERNMENT:
                verifiedTypeName = "蓝V-政府";
                break;
            case BLUE_V_ENTERPRISE:
                verifiedTypeName = "蓝V-企业";
                break;
            case BLUE_V_MEDIA:
                verifiedTypeName = "蓝V-媒体";
                break;
            case BLUE_V_SCHOOL:
                verifiedTypeName = "蓝V-校园";
                break;
            case BLUE_V_WEBSITE:
                verifiedTypeName = "蓝V-网站";
                break;
            case BLUE_V_APP:
                verifiedTypeName = "蓝V-应用";
                break;
            case BLUE_V_ORGANIZATION:
                verifiedTypeName = "蓝V-团体机构";
                break;
            case BLUE_V_ENTERPRISE_PENDING:
                verifiedTypeName = "蓝V-待审企业";
                break;
            case SHOW_V_WEIBO_GIRL:
                verifiedTypeName = "达人-微博女郎";
                break;
            case SHOW_V_PRIMARY:
                verifiedTypeName = "达人-初级达人";
                break;
            case SHOW_V_ADVANCED:
                verifiedTypeName = "达人-中高级达人";
                break;
            case BIG_V_DECEASED:
                verifiedTypeName = "已故V用户";
                break;
            default:
                verifiedTypeName = "其他";
                break;
        }
        return verifiedTypeName;
    }

    private Dictionary() {
    }
}
