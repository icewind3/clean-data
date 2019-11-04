package com.cl.data.social.mapper.weibo;

import com.cl.data.social.entity.UidValue;
import com.cl.data.social.mapper.marketing.UidValueMapper;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/9/3
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class UserInfoAddMapperTest {

    @Resource
    private UserInfoAddMapper userInfoAddMapper;

    @Resource
    private UidValueMapper uidValueMapper;

    @Test
    public void batchUpdateImg() {
        List<UidValue> uidValues = uidValueMapper.listAllUserImg();
        doBatchUpdate(uidValues, subList -> {
            userInfoAddMapper.batchUpdateImg(subList);
        });
    }

    @Test
    public void batchUpdateCreditScore() {
        List<UidValue> uidValues = uidValueMapper.listAllSunshine();
        doBatchUpdate(uidValues, subList -> {
            userInfoAddMapper.batchUpdateCreditScore(subList);
        });
    }

    @Test
    public void batchUpdateTags() {
        List<UidValue> uidValues = uidValueMapper.listAllTags();
        doBatchUpdate(uidValues, subList -> {
            userInfoAddMapper.batchUpdateTags(subList);
        });
    }

    @Test
    public void batchUpdateBirth() {
        List<UidValue> uidValues = uidValueMapper.listAllBirth();
        doBatchUpdate(uidValues, subList -> {
            userInfoAddMapper.batchUpdateBirth(subList);
        });
    }

    private void doBatchUpdate(List<UidValue> list, Consumer<List<UidValue>> consumer){
        int size = list.size();
        int fromIndex = 0;
        int step = 1000;
        while (fromIndex < size) {
            int toIndex = Math.min(size, fromIndex + step);
            List<UidValue> subValues = list.subList(fromIndex, toIndex);
            consumer.accept(subValues);
            System.out.println(fromIndex);
            fromIndex = toIndex;
        }
    }
}