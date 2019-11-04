package com.cl.data.social.mapper.marketing;

import com.cl.data.social.entity.UserZombieStatus;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author yejianyu
 * @date 2019/9/4
 */
public interface UserZombieStatusMapper {

//    List<UserZombieStatus> listAll(@Param("pageNum") int pageNum, @Param("pageSize") int pageSize);
    List<UserZombieStatus> listAll();

}
