package com.cl.data.hbase.service;

import com.cl.data.hbase.dto.UserBlogRepostStatDTO;
import com.cl.graph.weibo.core.util.CsvFileHelper;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/10/30
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class UserBlogRepostStatHbaseServiceTest {

    @Resource
    private UserBlogRepostStatHbaseService userBlogRepostStatHbaseService;

    @Test
    public void getUserBlogRepostStatMap() throws IOException {

        List<Long> uidList = new ArrayList<>();

        String uidFilePath = "C:\\Users\\cl32\\Documents\\weibo\\weiboBigGraphNew\\uid_core_new/uid_80w.csv";
        String resultPath = "C:\\Users\\cl32\\Desktop\\水军/uid_repost_ratio_80w.csv";
        try (CSVParser parser = CsvFileHelper.reader(uidFilePath, new String[]{}, true)) {
            for (CSVRecord record : parser) {
                Long uid = Long.parseLong(record.get(0));
                uidList.add(uid);
            }
        }

        String[] header = {"uid", "repost_ratio"};
        int maxCount = uidList.size();
        int pageSize = 1000;
        int pageNum = 1;
        int pageNumMax = (int) Math.ceil((double) maxCount / pageSize);
        try (CSVPrinter writer = CsvFileHelper.writer(resultPath, header)) {
            while (pageNum <= pageNumMax) {
                int fromIndex = (pageNum - 1) * pageSize;
                int toIndex = Math.min(pageNum * pageSize, maxCount);
                List<Long> subUidList = uidList.subList(fromIndex, toIndex);
                Map<Long, UserBlogRepostStatDTO> userBlogRepostStatMap = userBlogRepostStatHbaseService.getUserBlogRepostStatMap(subUidList);

                for (Long uid : subUidList) {
                    UserBlogRepostStatDTO userBlogRepostStat = userBlogRepostStatMap.get(uid);
                    if (userBlogRepostStat == null) {
                        System.out.println(uid);
                        continue;
                    }
                    writer.printRecord(uid, userBlogRepostStat.getRepostRatio());
                }
                System.out.println("number=" + pageNum);
                pageNum++;
            }
        }

    }
}