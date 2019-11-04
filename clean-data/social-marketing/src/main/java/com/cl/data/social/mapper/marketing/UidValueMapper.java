package com.cl.data.social.mapper.marketing;

import com.cl.data.social.entity.UidValue;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

/**
 * @author yejianyu
 * @date 2019/7/16
 */
@Mapper
public interface UidValueMapper {


    List<UidValue> listAllUserImg();
    List<UidValue> listAllSunshine();
    List<UidValue> listAllExamine();
    List<UidValue> listAllTags();
    List<UidValue> listAllBirth();



}
