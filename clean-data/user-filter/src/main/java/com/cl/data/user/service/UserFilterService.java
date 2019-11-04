package com.cl.data.user.service;

import com.cl.data.user.dto.UserBlogInfoDTO;
import com.cl.data.user.util.BlogContentUtils;
import com.cl.data.user.util.UserInfoUtils;
import com.cl.graph.weibo.core.util.CsvFileHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;

/**
 * @author yejianyu
 * @date 2019/9/16
 */
@Slf4j
@Service
public class UserFilterService {

    private static final String[] HEADER_USER_BLOG_STAT = {"uid", "releaseFrequency", "attitudeAvg", "commentAvg", "repostAvg",
        "attitudeMedian", "commentMedian", "repostMedian", "repostRate", "releaseFrequency2", "attitudeAvg2",
        "commentAvg2", "repostAvg2", "attitudeMedian2", "commentMedian2", "repostMedian2", "repostRate2", "attitudeTopAvg",
        "commentTopAvg", "repostTopAvg"};

    private static final String[] HEADER_USER_NAME_DESC = {"id","nick_name","desc"};

    @Resource
    private UidMidBlogHbaseService uidMidBlogHbaseService;

    public void filterBlacklist(String uidFilePath, String resultPath) {
        try (CSVParser csvParser = CsvFileHelper.reader(uidFilePath, HEADER_USER_BLOG_STAT, true);
             CSVPrinter printer = CsvFileHelper.writer(resultPath)) {
            for (CSVRecord record : csvParser) {
                String uid = record.get(0);
                System.out.println(uid);
                List<UserBlogInfoDTO> userBlogInfoList = uidMidBlogHbaseService.getUserBlogInfoList(uid);
                int blogSize = userBlogInfoList.size();
                if (blogSize == 0) {
                    continue;
                }
                int count = 0;
                for (UserBlogInfoDTO userBlogInfo : userBlogInfoList) {
                    String retweetedText = userBlogInfo.getRetweetedText();
                    if (StringUtils.isNotBlank(retweetedText)) {
                        if (BlogContentUtils.isSpam(retweetedText)) {
                            count++;
                        }
                    } else if (BlogContentUtils.isSpam(userBlogInfo.getText())) {
                        count++;
                    }
                }
                printer.printRecord(uid, ((float) count / blogSize));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean needFilter(String uid) {
        List<UserBlogInfoDTO> userBlogInfoList = uidMidBlogHbaseService.getUserBlogInfoList(uid);
        int blogSize = userBlogInfoList.size();
        if (userBlogInfoList.size() == 0) {
            return false;
        }
        int count = 0;
        for (UserBlogInfoDTO userBlogInfo : userBlogInfoList) {
            String retweetedText = userBlogInfo.getRetweetedText();
            if (StringUtils.isNotBlank(retweetedText)) {
                if (BlogContentUtils.isSpam(retweetedText)) {
                    count++;
                }
            } else if (BlogContentUtils.isSpam(userBlogInfo.getText())) {
                count++;
            }
        }
        return ((float) count / blogSize) >= 0.6;
    }

    public void filterByNameAndIntro(String userInfoPath, String resultPath) {
        try (CSVParser csvParser = CsvFileHelper.reader(userInfoPath, HEADER_USER_NAME_DESC, true);
             CSVPrinter printer = CsvFileHelper.writer(resultPath, HEADER_USER_NAME_DESC)) {
            int count = 0;
            for (CSVRecord record : csvParser) {
                String name = record.get(1);
                String desc = record.get(2);
                if (UserInfoUtils.isNameMatching(name) ){
                    count++;
                    printer.printRecord(record);
                }
            }
            log.info("名称简介匹配，共找到{}个账号", count);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void filterByNameAndIntro3(String userInfoPath, String resultPath) {
        try (CSVParser csvParser = CsvFileHelper.reader(userInfoPath, HEADER_USER_NAME_DESC, true);
             CSVPrinter printer = CsvFileHelper.writer(resultPath, HEADER_USER_NAME_DESC)) {
            int count = 0;
            for (CSVRecord record : csvParser) {
                String name = record.get(1);
                String desc = record.get(2);
                if (UserInfoUtils.isNameMatching2(name) ){
                    count++;
                    printer.printRecord(record);
                }
            }
            log.info("名称简介匹配，共找到{}个账号", count);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void filterByNameAndIntro2(String userInfoPath, String resultPath) {
        try (CSVParser csvParser = CsvFileHelper.reader(userInfoPath, new String[]{}, true);
             CSVPrinter printer = CsvFileHelper.writer(resultPath)) {
            int count = 0;
            for (CSVRecord record : csvParser) {
                String name = record.get(1);
                if (UserInfoUtils.isNameMatching(name) ){
                    count++;
                    printer.printRecord(record.get(0), name);
                }
            }
            log.info("名称简介匹配，共找到{}个账号", count);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
