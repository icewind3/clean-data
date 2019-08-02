package com.cl.graph.weibo.data.mapper.weibo;

import com.cl.graph.weibo.data.entity.UserInfo;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author yejianyu
 * @date 2019/7/24
 */
@Mapper
public interface UserInfoMapper {

    List<UserInfo> findAll(@Param("suffix") String tableSuffix, @Param("pageNum") int pageNum, @Param("pageSize") int pageSize);

    long count(@Param("suffix") String tableSuffix);
}
