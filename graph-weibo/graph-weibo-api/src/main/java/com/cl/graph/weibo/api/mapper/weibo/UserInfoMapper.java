package com.cl.graph.weibo.api.mapper.weibo;

import com.cl.graph.weibo.api.entity.CodeCount;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

/**
 * @author yejianyu
 * @date 2019/7/24
 */
@Mapper
public interface UserInfoMapper {

    List<CodeCount> groupByProvince(@Param("suffix") String tableSuffix);
    List<CodeCount> groupByGender(@Param("suffix") String tableSuffix);
    int isTableExist(@Param("tableName") String tableName);

    Map<String, String> checkTableExistsWithShow(@Param("tableName") String tableName);

    int count(@Param("suffix") String tableSuffix);
}
