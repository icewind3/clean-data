package com.cl.data.social.mapper.marketing;

import com.cl.data.social.entity.MblogFromUid;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author yejianyu
 * @date 2019/9/4
 */
public interface MblogFromUidMapper {

    MblogFromUid getByMid(@Param("tableName") String tableName, @Param("mid") String mid);

    int isTableExist(@Param("tableName") String tableName);

    List<MblogFromUid> listAll(@Param("tableName") String tableName, @Param("pageNum") int pageNum, @Param("pageSize") int pageSize);

    long count(@Param("tableName") String tableName);
}
