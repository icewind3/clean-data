package com.cl.graph.weibo.api.mapper.marketing;

import com.cl.graph.weibo.api.entity.HotComment;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author yejianyu
 * @date 2019/7/16
 */
@Mapper
public interface HotCommentMapper {

    List<HotComment> findAll(@Param("suffix") String tableSuffix, @Param("pageNum") int pageNum, @Param("pageSize") int pageSize);

    long count(@Param("suffix") String tableSuffix);

}
