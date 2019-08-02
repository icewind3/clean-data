package com.cl.graph.weibo.data.service;

import com.cl.graph.weibo.core.exception.ServiceException;
import com.cl.graph.weibo.data.entity.MblogFromUid;
import com.cl.graph.weibo.data.entity.MidUidText;
import com.cl.graph.weibo.data.mapper.marketing.MblogFromUidMapper;
import com.cl.graph.weibo.data.mapper.mblog.MidUidTextMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

/**
 * @author yejianyu
 * @date 2019/8/1
 */
@Slf4j
@Service
public class MidUidTextService {

    @Resource
    private MidUidTextMapper midUidTextMapper;

    private static final int TABLE_COUNT = 1000;

    public MidUidText insert(MidUidText midUidText) throws ServiceException{
        long index = midUidText.getUid() % TABLE_COUNT;
        try {
            midUidTextMapper.insert(midUidText, String.valueOf(index));
        } catch (DuplicateKeyException ignored){
        }
        return midUidText;
    }

    public MidUidText insertByMblogFromUid(MblogFromUid mblogFromUid) throws ServiceException{
        return this.insert(parse(mblogFromUid));
    }

    public List<MidUidText> insertAll(List<MidUidText> midUidTextList) throws ServiceException {
        midUidTextList.forEach(this::insert);
        return midUidTextList;
    }

    public long insertAll(Long uid, List<MidUidText> midUidTextList) throws ServiceException {
        long index = uid % TABLE_COUNT;
        return midUidTextMapper.insertAll(midUidTextList, String.valueOf(index));
    }

    public long insertAll(String index, List<MidUidText> midUidTextList) throws ServiceException {
        return midUidTextMapper.insertAll(midUidTextList, index);
    }

    public MidUidText parse(MblogFromUid mblogFromUid) {
        MidUidText midUidText = new MidUidText();
        BeanUtils.copyProperties(mblogFromUid, midUidText);
        midUidText.setUid(Long.valueOf(mblogFromUid.getUid()));
        boolean isRetweet = StringUtils.isNotBlank(mblogFromUid.getRetweetedMid());
        midUidText.setRetweeted(isRetweet);
        return midUidText;
    }
}
