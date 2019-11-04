package com.cl.data.social.mapper.marketing;

import com.cl.data.social.entity.MblogFromUid;
import com.cl.graph.weibo.core.util.DateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

import java.util.List;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/9/4
 */
@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest
public class MblogFromUidMapperTest {

    @Resource
    private MblogFromUidMapper mblogFromUidMapper;

    @Test
    public void getByMid() {
        String mid = "4383533961261025";
//        String mid = "4401897496812646";
        String table = "mblog_from_uid";
        List<String> dateList = DateTimeUtils.getDateList("20190603", "20190620");
//        List<String> dateList = DateTimeUtils.getDateList("20190711", "20190731");
//        List<String> dateList = DateTimeUtils.getDateList("20190802", "20190805");
        for (String date : dateList){
            for (int i = 0; i < 10; i++) {
                String tableName = table + "_" + date + "_" + i;
                try {
                    MblogFromUid mblogFromUid = mblogFromUidMapper.getByMid(tableName, mid);
                    if (mblogFromUid != null){
                        log.info("tableName={}", tableName);
                        log.info("mblogFromUid={}", mblogFromUid);
                    }
                } catch (Exception e){
                   log.warn("表不存在，{}",tableName);
                }

            }
        }
    }

    @Test
    public void getByMid2() {
        String mid = "4383533961261025";
        String table = "mblog_from_uid";
        List<String> dateList = DateTimeUtils.getDateList("20190731", "20190825");
        for (String date : dateList){
                String tableName = table + "_" + date;
                try {
                    MblogFromUid mblogFromUid = mblogFromUidMapper.getByMid(tableName, mid);
                    if (mblogFromUid != null){
                        log.info("tableName={}", tableName);
                        log.info("mblogFromUid={}", mblogFromUid);
                    }
                } catch (Exception e){
                    log.warn("表不存在，{}",tableName);
                }
        }
    }
}