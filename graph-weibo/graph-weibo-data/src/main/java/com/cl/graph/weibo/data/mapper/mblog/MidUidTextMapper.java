package com.cl.graph.weibo.data.mapper.mblog;

import com.cl.graph.weibo.data.entity.MidUidText;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author yejianyu
 * @date 2019/8/1
 */
@Mapper
public interface MidUidTextMapper {

    List<MidUidText> findAll(@Param("suffix") String tableSuffix, @Param("pageNum") int pageNum, @Param("pageSize") int pageSize);

    long count(@Param("suffix") String tableSuffix);

    int insert(@Param("blog") MidUidText midUidText, @Param("suffix") String tableSuffix);

    long insertAll(List<MidUidText> midUidTextList, @Param("suffix") String tableSuffix);
}
