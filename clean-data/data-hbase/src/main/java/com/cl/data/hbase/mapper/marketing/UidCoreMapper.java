package com.cl.data.hbase.mapper.marketing;

import org.apache.ibatis.annotations.Mapper;

import java.util.List;

/**
 * @author yejianyu
 * @date 2019/7/16
 */
@Mapper
public interface UidCoreMapper {

    List<Long> listAll();

}
