package com.cl.graph.weibo.data.mapper.marketing2;

import com.cl.graph.weibo.data.entity.UserAttitudes;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author yejianyu
 * @date 2019/7/16
 */
@Mapper
public interface UserAttitudesMapper {

    List<UserAttitudes> findAll(@Param("suffix") String tableSuffix, @Param("pageNum") int pageNum, @Param("pageSize") int pageSize);

    long count(@Param("suffix") String tableSuffix);

}
