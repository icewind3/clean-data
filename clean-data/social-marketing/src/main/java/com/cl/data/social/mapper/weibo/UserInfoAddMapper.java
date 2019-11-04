package com.cl.data.social.mapper.weibo;

import com.cl.data.social.entity.UidValue;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

/**
 * 更新用户信息中部分列
 * @author Ye Jianyu
 * @date 2019-09-03
 */
@Mapper
public interface UserInfoAddMapper {

    int batchUpdateImg(List<UidValue> uidValue);
    int batchUpdateCreditScore(List<UidValue> uidValue);
    int batchUpdateTags(List<UidValue> uidValue);
    int batchUpdateBirth(List<UidValue> uidValue);
}
