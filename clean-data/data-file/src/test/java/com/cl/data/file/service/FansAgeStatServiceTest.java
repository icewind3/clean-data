package com.cl.data.file.service;

import com.cl.graph.weibo.core.util.CsvFileHelper;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/10/8
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class FansAgeStatServiceTest {

    @Resource
    private FansAgeStatService fansAgeStatService;

    @Test
    public void computeFansAgeGroup() {
        String uidFilePath = "C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\fan_mblog\\10w_12w\\fans";
        String uidAgePath = "C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\userinfo_extend/uid_ageGroup_20191022.csv";
//        String resultPath = "C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\userinfo_extend/uid_age_stat.csv";
        String resultPath = "C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\userinfo_extend/uid_age_stat_10w_12w.csv";
        fansAgeStatService.computeFansAgeGroupStat2(uidFilePath, uidAgePath, resultPath);
    }

    @Test
    public void genUidAgeGroupFile() {
        String resultPath = "C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\userinfo_extend/uid_ageGroup_20191022.csv";
        String filePath = "C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\userinfo_extend\\14";
        File oriFile = new File(filePath);
        File[] files = oriFile.listFiles();
        for (File file : files) {
            fansAgeStatService.genUidAgeGroupFile(file.getPath(), resultPath);
        }
    }

    @Test
    public void test() throws IOException {
        try (CSVParser csvParser = CsvFileHelper.reader("C:\\Users\\cl32\\Desktop\\userinfo_extend_20190826.csv", new String[]{}, true)){
            for (CSVRecord record : csvParser) {
                String birth = record.get(1);
                if (StringUtils.isBlank(birth)) {
                    continue;
                }
                String[] array = birth.split(" ");
                String birthDay = array[0];
                String dateRegex = "^\\d{4}-\\d{2}-\\d{2}$";
                if (Pattern.matches(dateRegex, birthDay)){
                    String substring = birthDay.substring(0, 3);
                    if ("190".equals(substring) || "191".equals(substring)) {
                        continue;
                    }
                    String century = birthDay.substring(0, 2);
                    if ("19".equals(century) || "20".equals(century)) {
                        String decade = birthDay.substring(2, 3);
                        String ageGroup = decade + "0后";
                        if ("00后".equals(ageGroup)){
                            System.out.println(birth);
                        }
                    }
                } else {
//                    System.out.println(birth);
                }
            }
        }
//        String birth = "1986-11-06";

    }
}