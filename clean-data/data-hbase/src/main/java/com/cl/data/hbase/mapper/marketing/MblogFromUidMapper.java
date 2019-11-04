package com.cl.data.hbase.mapper.marketing;

import com.cl.data.hbase.entity.MblogFromUid;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author yejianyu
 * @date 2019/7/16
 */
@Mapper
public interface MblogFromUidMapper{


    int isTableExist(@Param("tableName") String tableName);

    List<MblogFromUid> findAllBid(@Param("tableName") String tableName, @Param("pageNum") int pageNum,
                               @Param("pageSize") int pageSize);

    List<MblogFromUid> findAllRetweetedMid(@Param("tableName") String tableName, @Param("pageNum") int pageNum,
                                  @Param("pageSize") int pageSize);

    long count(@Param("tableName") String tableName);
}
