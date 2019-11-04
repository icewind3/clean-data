package com.cl.data.social.mapper.weibo;

import com.cl.data.social.entity.UserIndicator;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * 更新用户信息中部分列
 * @author Ye Jianyu
 * @date 2019-09-03
 */
@Mapper
public interface UserIndicatorMapper {

    int insertAll(List<UserIndicator> userIndicatorList);

    int insertRepostRatio(List<UserIndicator> userIndicatorList);

    int insertAllBci(@Param(value = "tableName") String tableName,
                     @Param(value = "list") List<UserIndicator> userIndicatorList);

}
