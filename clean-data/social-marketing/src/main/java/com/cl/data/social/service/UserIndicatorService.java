package com.cl.data.social.service;

import com.cl.data.social.entity.UserIndicator;
import com.cl.data.social.entity.UserZombieStatus;
import com.cl.data.social.mapper.marketing.UserZombieStatusMapper;
import com.cl.data.social.mapper.weibo.UserIndicatorMapper;
import com.cl.graph.weibo.core.util.CsvFileHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yejianyu
 * @date 2019/10/25
 */
@Slf4j
@Service
public class UserIndicatorService {

    @Resource
    private UserZombieStatusMapper zombieStatusMapper;

    @Resource
    private UserIndicatorMapper userIndicatorMapper;

    public void computeZombieRatio() {
        List<UserZombieStatus> userZombieStatusList = zombieStatusMapper.listAll();
        List<UserIndicator> userIndicatorList = new ArrayList<>();
        userZombieStatusList.forEach(userZombieStatus -> {
            Integer total = userZombieStatus.getTotal();
            if (total <= 0) {
                return;
            }
            Integer zombieTotal = userZombieStatus.getZombieTotal();
            float realZombieTotal = zombieTotal + userZombieStatus.getSuspectedLevel1() * 0.15f
                + userZombieStatus.getSuspectedLevel1() * 0.218f + userZombieStatus.getSuspectedLevel1() * 0.1935f;
            UserIndicator userIndicator = new UserIndicator();
            userIndicator.setUid(userZombieStatus.getUid());
            userIndicator.setZombieRatio(realZombieTotal / total);
            userIndicatorList.add(userIndicator);
            if (userIndicatorList.size() >= 1000) {
                userIndicatorMapper.insertAll(userIndicatorList);
                userIndicatorList.clear();
            }
        });
        if (userIndicatorList.size() > 0) {
            userIndicatorMapper.insertAll(userIndicatorList);
        }
    }

    public void insertRepostRatio(String filePath) {
        String[] header = {"uid", "repost_ratio"};
        List<UserIndicator> userIndicatorList = new ArrayList<>();
        int count = 0;
        try (CSVParser csvParser = CsvFileHelper.reader(filePath, header, true)) {
            for (CSVRecord record : csvParser) {
                UserIndicator userIndicator = new UserIndicator();
                userIndicator.setUid(Long.parseLong(record.get(0)));
                userIndicator.setRepostRatio(Float.parseFloat(record.get(1)));
                userIndicatorList.add(userIndicator);
                count++;
                if (userIndicatorList.size() >= 1000) {
                    userIndicatorMapper.insertRepostRatio(userIndicatorList);
                    userIndicatorList.clear();
                    log.info("complete {}", count);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (userIndicatorList.size() > 0) {
            userIndicatorMapper.insertRepostRatio(userIndicatorList);
        }
        log.info("end {}", count);
    }

    public void insertBci(String filePath) {
        String[] header = {"uid", "start_date", "end_date", "period_sign", "bci"};
        List<UserIndicator> userIndicatorList = new ArrayList<>();
//        String month = "2019-09";
        String periodSign = "43";
        String tableName = "user_bci_2019_" + periodSign;
        int count = 0;
        int max = 0;
        try (CSVParser csvParser = CsvFileHelper.reader(filePath, header, true)) {
            for (CSVRecord record : csvParser) {
                String sign = record.get(3);
                max = Math.max(Integer.parseInt(record.get(5)), max);
                if (!StringUtils.equals(periodSign, sign)){
                    continue;
                }
                UserIndicator userIndicator = new UserIndicator();
                userIndicator.setUid(Long.parseLong(record.get(0)));
                userIndicator.setBci(Float.parseFloat(record.get(4)));
                userIndicator.setStartDate(record.get(1));
                userIndicator.setEndDate(record.get(2));
                userIndicatorList.add(userIndicator);
                count++;
                if (userIndicatorList.size() >= 1000) {
                    userIndicatorMapper.insertAllBci(tableName, userIndicatorList);
                    userIndicatorList.clear();
                    log.info("complete {}", count);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (userIndicatorList.size() > 0) {
            userIndicatorMapper.insertAllBci(tableName, userIndicatorList);
        }
        log.info("end {}", count);
        log.info("max={}", max);
    }
}
