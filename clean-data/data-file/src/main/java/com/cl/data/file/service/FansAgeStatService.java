package com.cl.data.file.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.cl.graph.weibo.core.util.CsvFileHelper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @author yejianyu
 * @date 2019/10/8
 */
@Service
@Slf4j
public class FansAgeStatService {

    private static final String DATE_REGEX = "^\\d{4}-\\d{2}-\\d{2}$";

    public void computeFansAgeGroupStat(String uidFilePath, String uidAgeFilePath, String resultPath) {
        File uidFile = new File(uidFilePath);
        List<File> fileList = new ArrayList<>();
        if (uidFile.isDirectory()) {
            File[] files = uidFile.listFiles();
            if (files != null) {
                Collections.addAll(fileList, files);
            }
        } else {
            fileList.add(uidFile);
        }
        try (CSVPrinter csvPrinter = CsvFileHelper.writer(resultPath, true)){
            for (File file : fileList) {
                String uid = file.getName();
                Set<String> uidSet = new HashSet<>();
                try (CSVParser csvParser = CsvFileHelper.reader(file)) {
                    for (CSVRecord record : csvParser) {
                        uidSet.add(record.get(0));
                    }
                }
                UidAgeStat uidAgeStat = computeFansAgeGroup(uidAgeFilePath, uidSet);
                csvPrinter.printRecord(uid, uidAgeStat.getFansCount(), uidAgeStat.getHasAgeCount(),
                    JSON.toJSONString(uidAgeStat.getAgeGroupCount(), SerializerFeature.UseSingleQuotes));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void computeFansAgeGroupStat2(String uidFilePath, String uidAgeFilePath, String resultPath) {
        File uidFile = new File(uidFilePath);
        List<File> fileList = new ArrayList<>();
        if (uidFile.isDirectory()) {
            File[] files = uidFile.listFiles();
            if (files != null) {
                Collections.addAll(fileList, files);
            }
        } else {
            fileList.add(uidFile);
        }

        Map<String, String> uidAgeGroupMap = new HashMap<>(67528857);
        try (CSVParser csvParser = CsvFileHelper.reader(uidAgeFilePath)) {
            for (CSVRecord record : csvParser) {
                uidAgeGroupMap.put(record.get(0), record.get(1));
            }
            log.info("年龄层信息加载完成, size={}", uidAgeGroupMap.size());
        }catch (IOException e) {
            e.printStackTrace();
        }

        try (CSVPrinter csvPrinter = CsvFileHelper.writer(resultPath, true);
             CSVPrinter csvPrinter2 = CsvFileHelper.writer(resultPath + "2", true)){
            for (File file : fileList) {
                String uid = file.getName();
                Set<String> uidSet = new HashSet<>();
                try (CSVParser csvParser = CsvFileHelper.reader(file)) {
                    for (CSVRecord record : csvParser) {
                        uidSet.add(record.get(0));
                    }
                }
                UidAgeStat uidAgeStat = computeFansAgeGroup(uidAgeGroupMap, uidSet);
                int fansCount = uidAgeStat.getFansCount();
                int hasAgeCount = uidAgeStat.getHasAgeCount();
                Map<String, Integer> ageGroupCount = uidAgeStat.getAgeGroupCount();
                csvPrinter.printRecord(uid, fansCount, hasAgeCount,
                    JSON.toJSONString(ageGroupCount, SerializerFeature.UseSingleQuotes));

                Map<String, Float> ageGroupPercent = new HashMap<>();
                ageGroupCount.forEach((ageGroup, count) -> {
                    ageGroupPercent.put(ageGroup, (float) count / hasAgeCount);
                });
                csvPrinter2.printRecord(uid, fansCount, (float) hasAgeCount / fansCount,
                    JSON.toJSONString(ageGroupPercent, SerializerFeature.UseSingleQuotes));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public UidAgeStat computeFansAgeGroup(String filePath, Set<String> uidSet) {
        int fansCount = uidSet.size();
        int hasAgeCount = 0;
        Set<String> fansUidSet = new HashSet<>(uidSet);
        Map<String, Integer> map = new HashMap<>();
        try (CSVParser csvParser = CsvFileHelper.reader(filePath)) {
            for (CSVRecord record : csvParser) {
                String uid = record.get(0);
                if (fansUidSet.contains(uid)) {
                    hasAgeCount++;
                    String ageGroup = record.get(1);
                    map.merge(ageGroup, 1, Integer::sum);
                    fansUidSet.remove(uid);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        UidAgeStat uidAgeStat = new UidAgeStat(fansCount, hasAgeCount);
        uidAgeStat.setAgeGroupCount(map);
        return uidAgeStat;
    }

    public UidAgeStat computeFansAgeGroup(Map<String, String> uidAgeGroupMap, Set<String> uidSet) {
        int fansCount = uidSet.size();
        int hasAgeCount = 0;
        Map<String, Integer> map = new HashMap<>();
        for (String uid : uidSet){
            if (uidAgeGroupMap.containsKey(uid)) {
                hasAgeCount++;
                String ageGroup = uidAgeGroupMap.get(uid);
                map.merge(ageGroup, 1, Integer::sum);
            }
        }
        UidAgeStat uidAgeStat = new UidAgeStat(fansCount, hasAgeCount);
        uidAgeStat.setAgeGroupCount(map);
        return uidAgeStat;
    }


    public void genUidAgeGroupFile(String filePath, String resultPath) {
        File resultFile = new File(resultPath);
        boolean append = false;
        if (resultFile.exists()) {
            append = true;
        }
        try (CSVParser csvParser = CsvFileHelper.reader(filePath, new String[]{}, true);
             CSVPrinter csvPrinter = CsvFileHelper.writer(resultFile, append)) {
            for (CSVRecord record : csvParser) {
                String birth = record.get(1);
                if (StringUtils.isBlank(birth)) {
                    continue;
                }
                String[] array = birth.split(" ");
                String birthDay = array[0];
                String ageGroup = getAgeGroup(birthDay);
                if (StringUtils.isNotBlank(ageGroup)) {
                    csvPrinter.printRecord(record.get(0), ageGroup);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getAgeGroup(String birthDay) {
        String ageGroup = StringUtils.EMPTY;
        if (Pattern.matches(DATE_REGEX, birthDay)) {
            String substring = birthDay.substring(0, 3);
            if ("190".equals(substring) || "191".equals(substring)) {
                return ageGroup;
            }
            String century = birthDay.substring(0, 2);

            if ("19".equals(century) || "20".equals(century)) {
                String decade = birthDay.substring(2, 3);
                ageGroup = decade + "0后";
            }
        }
        return ageGroup;
    }

    @Data
    class UidAgeStat {
        int fansCount;
        int hasAgeCount;
        Map<String, Integer> ageGroupCount;

        UidAgeStat(int fansCount, int hasAgeCount){
            this.fansCount = fansCount;
            this.hasAgeCount = hasAgeCount;
        }
    }
}
