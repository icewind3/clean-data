package com.cl.data.stat.service;

import com.cl.data.stat.dictionary.Dictionary;
import com.cl.data.stat.dictionary.ProvinceDictionary;
import com.cl.graph.weibo.core.util.CsvFileHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.*;


/**
 * @author yejianyu
 * @date 2019/9/6
 */
@Slf4j
@Service
public class UserInfoStatService {

    private static final String[] HEADER_USER_INFO = {"uid", "name", "gender", "province", "city", "location",
        "verified_type", "verified_type_ext"};

    public void countUserInfoStat(String userInfoFilePath, String resultPath) {
        Map<String, Integer> genderMap = new HashMap<>(3);
        Map<String, Integer> provinceMap = new HashMap<>(49);
        Map<String, Integer> userTypeMap = new HashMap<>(21);
        int userCount = 0;
        long startTime = System.currentTimeMillis();
        log.info("开始统计用户信息");
        try (CSVParser csvParser = CsvFileHelper.reader(userInfoFilePath, HEADER_USER_INFO, true)) {
            for (CSVRecord record : csvParser) {
                String gender = record.get(2);
                genderMap.merge(gender, 1, Integer::sum);

                String province = record.get(3);
                provinceMap.merge(province, 1, Integer::sum);

                String verifiedType = record.get(6);
                String verifiedTypeExt = record.get(7);
                Integer verifiedTypeInt =
                    StringUtils.isBlank(verifiedType) ? null : Integer.parseInt(verifiedType);
                Integer verifiedTypeExtInt =
                    StringUtils.isBlank(verifiedTypeExt) ? null : Integer.parseInt(verifiedTypeExt);
                String verifiedTypeName = Dictionary.getVerifiedTypeName(verifiedTypeInt, verifiedTypeExtInt);
                userTypeMap.merge(verifiedTypeName, 1, Integer::sum);
                userCount++;
                if (userCount % 1000000 == 0) {
                    log.info("统计用户信息, count={}，time= {}ms", userCount, System.currentTimeMillis() - startTime);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (CSVPrinter genderWriter = CsvFileHelper.writer(resultPath + File.separator + "genderCount.csv");
             CSVPrinter provinceWriter = CsvFileHelper.writer(resultPath + File.separator + "provinceCount.csv");
             CSVPrinter userTypePrinter = CsvFileHelper.writer(resultPath + File.separator + "userTypeCount.csv")) {
            genderMap.forEach((key, count) -> {
                try {
                    genderWriter.printRecord(Dictionary.getGenderName(key), count);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            List<Map.Entry<String, Integer>> provinceList = descSortMap(provinceMap);
            provinceList.forEach(entry -> {
                try {
                    provinceWriter.printRecord(ProvinceDictionary.getProvince(entry.getKey()), entry.getValue());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            userTypeMap.forEach((key, count) -> {
                try {
                    userTypePrinter.printRecord(key, count);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("统计用户信息完成，count={}，time= {}ms", userCount, System.currentTimeMillis() - startTime);
    }

    public void countUserInfoGenderStat(String userInfoFilePath, String resultPath) {
        Map<String, Integer> genderMap = new HashMap<>(3);
        int userCount = 0;
        long startTime = System.currentTimeMillis();
        log.info("开始统计用户性别");
        try (CSVParser csvParser = CsvFileHelper.reader(userInfoFilePath, HEADER_USER_INFO, true)) {
            for (CSVRecord record : csvParser) {
                String gender = record.get(2);

                String verifiedType = record.get(6);
                Integer verifiedTypeInt =
                    StringUtils.isBlank(verifiedType) ? null : Integer.parseInt(verifiedType);
                if (verifiedTypeInt == null) {
                    log.warn("用户认证类型为空，uid={}, gender={}", record.get(0), gender);
                }
                if (verifiedTypeInt == null || verifiedTypeInt < 1 || verifiedTypeInt > 8){
                    genderMap.merge(gender, 1, Integer::sum);
                    userCount++;
                }
                if (userCount % 1000000 == 0) {
                    log.info("统计用户信息, count={}，time= {}ms", userCount, System.currentTimeMillis() - startTime);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (CSVPrinter genderWriter = CsvFileHelper.writer(resultPath + File.separator + "genderCount.csv")) {
            genderMap.forEach((key, count) -> {
                try {
                    genderWriter.printRecord(Dictionary.getGenderName(key), count);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("统计用户信息完成，count={}，time= {}ms", userCount, System.currentTimeMillis() - startTime);
    }

    private List<Map.Entry<String, Integer>> descSortMap(Map<String, Integer> countMap) {
        List<Map.Entry<String, Integer>> list = new LinkedList<>(countMap.entrySet());
        list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));
        return list;
    }
}
