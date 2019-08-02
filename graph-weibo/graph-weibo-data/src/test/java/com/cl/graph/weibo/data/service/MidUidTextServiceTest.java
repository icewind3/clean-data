package com.cl.graph.weibo.data.service;

import com.cl.graph.weibo.data.BaseSpringBootTest;
import com.cl.graph.weibo.data.entity.MblogFromUid;
import com.cl.graph.weibo.data.entity.MidUidText;
import com.cl.graph.weibo.data.mapper.marketing.MblogFromUidMapper;
import com.cl.graph.weibo.data.mapper.mblog.MidUidTextMapper;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.test.annotation.Rollback;

import javax.annotation.Resource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/8/1
 */
public class MidUidTextServiceTest extends BaseSpringBootTest {

    @Resource
    private MblogFromUidMapper mblogFromUidMapper;

    @Resource
    private MidUidTextMapper midUidTextMapper;

    @Test
    public void test() {
        List<MblogFromUid> all = mblogFromUidMapper.findAll("20190317_0", 1, 20);
        Map<Long, List<MidUidText>> map = new HashMap<>();
        all.forEach(mblogFromUid -> {
            String uid = mblogFromUid.getUid();
            Long uidLong = Long.parseLong(uid);
            if (map.containsKey(uidLong)) {
                List<MidUidText> midUidTextList = map.get(uidLong);
                midUidTextList.add(parse(mblogFromUid));
            } else {
                List<MidUidText> midUidTextList = new ArrayList<>();
                midUidTextList.add(parse(mblogFromUid));
                map.put(uidLong, midUidTextList);
            }
        });
        map.forEach((uid, midUidTexts) -> {
            long index = uid % 2;
            String suffix = String.valueOf(index);
            for (MidUidText midUidText : midUidTexts) {
                try {
                    int l = midUidTextMapper.insert(midUidText, suffix);
                    System.out.println(uid + ": " + l);

                } catch (DuplicateKeyException e) {
                    System.out.println(midUidText.getMid());
                }
            }
        });
    }

    private MidUidText parse(MblogFromUid mblogFromUid) {
        MidUidText midUidText = new MidUidText();
        BeanUtils.copyProperties(mblogFromUid, midUidText);
        midUidText.setUid(Long.valueOf(mblogFromUid.getUid()));
        boolean isRetweet = StringUtils.isNotBlank(mblogFromUid.getRetweetedMid());
        midUidText.setRetweeted(isRetweet);
        return midUidText;
    }

}