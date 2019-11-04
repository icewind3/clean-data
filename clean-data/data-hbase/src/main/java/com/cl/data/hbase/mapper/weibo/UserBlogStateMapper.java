package com.cl.data.hbase.mapper.weibo;

import com.cl.data.hbase.entity.UserBlogState;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author yejianyu
 * @date 2019/7/16
 */
@Mapper
public interface UserBlogStateMapper {

    int insert(UserBlogState userBlogState);

    long insertAll(List<UserBlogState> userBlogStateList);

}
