package com.cl.graph.weibo.api.service;

import com.cl.graph.weibo.api.dictionary.ProvinceDictionary;
import com.cl.graph.weibo.api.dto.RealmCountDTO;
import com.cl.graph.weibo.api.entity.CodeCount;
import com.cl.graph.weibo.api.mapper.weibo.UserInfoMapper;
import com.cl.graph.weibo.core.exception.ServiceException;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author yejianyu
 * @date 2019/7/26
 */
@Service
public class UserInfoService {

    @Resource
    private UserInfoMapper userInfoMapper;

    public List<RealmCountDTO> listProvinceCounts(String tableSuffix) throws ServiceException {
        checkTable(tableSuffix);
        List<CodeCount> codeCounts = userInfoMapper.groupByProvince(tableSuffix);
        return codeCounts.stream().map(codeCount -> {
            String name = ProvinceDictionary.getProvince(codeCount.getCode());
            return RealmCountDTO.build(name, codeCount.getCount());
        }).collect(Collectors.toList());
    }

    public List<RealmCountDTO> listGenderCounts(String tableSuffix) throws ServiceException {
        checkTable(tableSuffix);
        List<CodeCount> codeCounts = userInfoMapper.groupByGender(tableSuffix);
        return codeCounts.stream().map(codeCount -> {
            String name = codeCount.getCode();
            return RealmCountDTO.build(name, codeCount.getCount());
        }).collect(Collectors.toList());
    }

    private void checkTable(String tableSuffix){
        String tableName = "userinfo_" + tableSuffix;
        int tableExist = userInfoMapper.isTableExist(tableSuffix);
        if (tableExist <= 0) {
            throw new ServiceException("表 " + tableName + " 不存在");
        }
    }
}
