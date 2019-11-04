package com.cl.data.hbase.service;

import com.cl.data.hbase.dto.BlogDTO;
import com.cl.data.hbase.dto.UserBlogListDTO;
import com.cl.graph.weibo.core.util.CsvFileHelper;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
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
import java.util.*;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/10/18
 */
@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest
public class UserBlogSearchHbaseServiceTest {

    @Resource
    private UserBlogSearchHbaseService userBlogSearchHbaseService;

    @Test
    public void getUserBlogListMap() throws IOException {
//        List<Long> uidList  = Lists.newArrayList(6419218390L);
        List<Long> uidList = new ArrayList<>();

        String uidFilePath = "C:\\Users\\cl32\\Desktop\\AllBirds/uid_kol_287.txt";
        String resultPath = "C:\\Users\\cl32\\Desktop\\AllBirds/top3Blog";
        try (CSVParser parser = CsvFileHelper.reader(uidFilePath)) {
            for (CSVRecord record : parser) {
                uidList.add(Long.parseLong(record.get(0)));
            }
        }

        Map<Long, UserBlogListDTO> userBlogListMap = userBlogSearchHbaseService.getUserBlogListMap(uidList);
        String[] header = new String[]{"uid", "mid", "text", "retweetedText"};
        try (CSVPrinter attitudeWriter = CsvFileHelper.writer(resultPath + File.separator + "uid_blog_top3_attitude.csv", header);
             CSVPrinter commentWriter = CsvFileHelper.writer(resultPath + File.separator + "uid_blog_top3_comment.csv", header);
             CSVPrinter repostWriter = CsvFileHelper.writer(resultPath + File.separator + "uid_blog_top3_repost.csv", header)) {
            for (Long uid : uidList) {
                UserBlogListDTO userBlogListDTO = userBlogListMap.get(uid);
                write(attitudeWriter, userBlogListDTO.getAttitudeBlogList());
                write(commentWriter, userBlogListDTO.getCommentBlogList());
                write(repostWriter, userBlogListDTO.getRepostBlogList());
            }
        }
    }

    private void write(CSVPrinter writer, List<BlogDTO> list) {
        list.forEach(blogDTO -> {
            try {
                writer.printRecord(blogDTO.getUid(), blogDTO.getMid(), blogDTO.getText(), blogDTO.getRetweetedText());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void getUserBlogListMap2() throws IOException {
//        List<Long> uidList  = Lists.newArrayList(6419218390L);
        List<Long> uidList = new ArrayList<>();

        String uidFilePath = "C:\\Users\\cl32\\Documents\\weibo\\weiboBigGraphNew\\uid_core_new_80w/uid_80w.csv";
        String resultPath = "C:\\Users\\cl32\\Desktop\\分词20191101\\top50";
//        try (CSVParser parser = CsvFileHelper.reader(uidFilePath)) {
//            for (CSVRecord record : parser) {
//                uidList.add(Long.parseLong(record.get(0)));
//            }
//        }

        int count = 0;
        try (CSVParser parser = CsvFileHelper.reader(uidFilePath);
             CSVPrinter attitudeWriter = CsvFileHelper.writer(resultPath + File.separator + "uid_mid_top50_attitude.csv");
             CSVPrinter commentWriter = CsvFileHelper.writer(resultPath + File.separator + "uid_mid_top50_comment.csv");
             CSVPrinter repostWriter = CsvFileHelper.writer(resultPath + File.separator + "uid_mid_top50_repost.csv")) {
            for (CSVRecord record : parser) {
                uidList.add(Long.parseLong(record.get(0)));
                count++;
                if (uidList.size() >= 1000) {
                    try {
                        Map<Long, UserBlogListDTO> userBlogListMap = userBlogSearchHbaseService.getUserBlogListMap(uidList);
                        for (Long uid : uidList) {
                            UserBlogListDTO userBlogListDTO = userBlogListMap.get(uid);
                            if (userBlogListDTO == null){
                                continue;
                            }
                            writeUidMid(attitudeWriter, userBlogListDTO.getAttitudeBlogList());
                            writeUidMid(commentWriter, userBlogListDTO.getCommentBlogList());
                            writeUidMid(repostWriter, userBlogListDTO.getRepostBlogList());
                        }
                        uidList.clear();
                        log.info("complete {}", count);
                    } catch (Exception e) {
                        attitudeWriter.flush();
                        commentWriter.flush();
                        repostWriter.flush();
                        log.error("uid={}", uidList.get(0), e);
                        try {
                            Thread.sleep(30000L);
                        } catch (InterruptedException ex) {
                            ex.printStackTrace();
                        }
                    }
                }
            }
            if (uidList.size() > 0) {
                Map<Long, UserBlogListDTO> userBlogListMap = userBlogSearchHbaseService.getUserBlogListMap(uidList);
                for (Long uid : uidList) {
                    UserBlogListDTO userBlogListDTO = userBlogListMap.get(uid);
                    writeUidMid(attitudeWriter, userBlogListDTO.getAttitudeBlogList());
                    writeUidMid(commentWriter, userBlogListDTO.getCommentBlogList());
                    writeUidMid(repostWriter, userBlogListDTO.getRepostBlogList());
                }
            }
        }
        log.info("success {}", count);
    }

    @Test
    public void getUserBlogMedianListMap() throws IOException {
//        List<Long> uidList  = Lists.newArrayList(6419218390L);
        List<Long> uidList = new ArrayList<>();

        String uidFilePath = "C:\\Users\\cl32\\Desktop\\AllBirds/uid_kol_287.txt";
        String resultPath = "C:\\Users\\cl32\\Desktop\\AllBirds\\median30Blog";
        try (CSVParser parser = CsvFileHelper.reader(uidFilePath)) {
            for (CSVRecord record : parser) {
                uidList.add(Long.parseLong(record.get(0)));
            }
        }

        Map<Long, UserBlogListDTO> userBlogListMap = userBlogSearchHbaseService.getUserBlogMedianListMap(uidList);
        try (CSVPrinter attitudeWriter = CsvFileHelper.writer(resultPath + File.separator + "uid_mid_median30_attitude.csv");
             CSVPrinter commentWriter = CsvFileHelper.writer(resultPath + File.separator + "uid_mid_median30_comment.csv");
             CSVPrinter repostWriter = CsvFileHelper.writer(resultPath + File.separator + "uid_mid_median30_repost.csv")) {
            for (Long uid : uidList) {
                UserBlogListDTO userBlogListDTO = userBlogListMap.get(uid);
                writeUidMid(attitudeWriter, userBlogListDTO.getAttitudeBlogList());
                writeUidMid(commentWriter, userBlogListDTO.getCommentBlogList());
                writeUidMid(repostWriter, userBlogListDTO.getRepostBlogList());
            }
        }
    }

    private void writeUidMid(CSVPrinter writer, List<BlogDTO> list) {
        list.forEach(blogDTO -> {
            try {
                writer.printRecord(blogDTO.getUid(), blogDTO.getMid());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }


    @Test
    public void getUserNewestBlogList() throws IOException {
        List<Long> uidList = new ArrayList<>();
        Map<Long, String> uidDomainMap = new HashMap<>();

        String uidFilePath = "C:\\Users\\cl32\\Desktop\\获取kol最新100篇博文/lableUser100Id.csv";
        String resultPath = "C:\\Users\\cl32\\Desktop\\获取kol最新100篇博文";
        try (CSVParser parser = CsvFileHelper.reader(uidFilePath, new String[]{}, true)) {
            for (CSVRecord record : parser) {
                Long uid = Long.parseLong(record.get(0));
                uidList.add(uid);
                uidDomainMap.put(uid, record.get(1));
            }
        }

        String[] header = {"uid", "label", "text", "retweeted_text"};
        int maxCount = uidList.size();
        int pageSize = 100;
        int pageNum = 1;
        int pageNumMax = (int) Math.ceil((double) maxCount / pageSize);
        while (pageNum <= pageNumMax) {
            int fromIndex = (pageNum - 1) * pageSize;
            int toIndex = Math.min(pageNum * pageSize, maxCount);
            List<Long> subUidList = uidList.subList(fromIndex, toIndex);
            Map<Long, UserBlogListDTO> userBlogListMap = userBlogSearchHbaseService.getUserNewestBlogListMap(subUidList, 100);
            try (CSVPrinter writer = CsvFileHelper.writer(resultPath + File.separator + "lableUser100Blog_"
                + pageNum + ".csv", header, true)) {
                for (Long uid : subUidList) {
                    UserBlogListDTO userBlogListDTO = userBlogListMap.get(uid);
                    if (userBlogListDTO == null) {
                        System.out.println(uid);
                        continue;
                    }
                    for (BlogDTO blogDTO : userBlogListDTO.getBlogList()) {
                        writer.printRecord(blogDTO.getUid(), uidDomainMap.get(uid), blogDTO.getText(),
                            blogDTO.getRetweetedText());
                    }
                }
            }
            System.out.println("number=" + pageNum);
            pageNum++;
        }
    }

    @Test
    public void getUserNewestBlogList2() throws IOException {
        Map<String, Set<Long>> domainUidMap = new HashMap<>();

        String uidFilePath = "C:\\Users\\cl32\\Desktop\\获取kol最新100篇博文/lableUser100Id.csv";
        String resultPath = "C:\\Users\\cl32\\Desktop\\获取kol最新100篇博文/uid_label_text";
        try (CSVParser parser = CsvFileHelper.reader(uidFilePath, new String[]{}, true)) {
            for (CSVRecord record : parser) {
                Long uid = Long.parseLong(record.get(0));
                String domain = record.get(1);
                if (domainUidMap.containsKey(domain)) {
                    domainUidMap.get(domain).add(uid);
                } else {
                    Set<Long> set = new HashSet<>();
                    set.add(uid);
                    domainUidMap.put(domain, set);
                }
            }
        }
        String[] header = {"uid", "label", "text", "retweeted_text"};
        domainUidMap.forEach((domain, uidSet) -> {
            System.out.println("start " + domain);
            Map<Long, UserBlogListDTO> userBlogListMap = userBlogSearchHbaseService.getUserNewestBlogListMap(Lists.newArrayList(uidSet), 100);
            try (CSVPrinter writer = CsvFileHelper.writer(resultPath + File.separator + "uid_label_text_"
                + domain + ".csv", header, true)) {
                for (Long uid : uidSet) {
                    UserBlogListDTO userBlogListDTO = userBlogListMap.get(uid);
                    if (userBlogListDTO == null) {
                        System.out.println(uid);
                        continue;
                    }
                    for (BlogDTO blogDTO : userBlogListDTO.getBlogList()) {
                        writer.printRecord(blogDTO.getUid(), domain, blogDTO.getText(),
                            blogDTO.getRetweetedText());
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("label complete" + domain);
        });
    }

    private Set<String> getSet() {
        Set<String> set = new HashSet<>();
        set.add("航空");
        set.add("工农贸易");
        set.add("美食");
        set.add("国学");
        set.add("汽车");
        set.add("萌宠");
        set.add("武术");
        set.add("商务服务");
        set.add("动漫");
        set.add("媒体");
        set.add("房地产");
        set.add("健康养生");
        set.add("日用百货");
        set.add("人文艺术");
        set.add("社会团体");
        set.add("数码");
        set.add("运动健身");
        set.add("旅游");
        set.add("摄影");
        set.add("婚庆服务");
        set.add("书法");
        set.add("历史");
        set.add("宗教");
        set.add("机构场所");
        set.add("音乐");
        set.add("政府");
        set.add("设计");
        set.add("娱乐");
        set.add("体育");
        set.add("互联网");
        set.add("三农");
        set.add("家居");
        set.add("电影");
        set.add("国画");
        set.add("公益");
        set.add("生活服务");
        set.add("收藏");
        set.add("健康医疗");
        set.add("游戏");
        set.add("读书");
        set.add("军事");
        set.add("法律");
        set.add("时尚美妆");
        set.add("校园");
        set.add("职场");
        set.add("星座");
        set.add("美术");
        set.add("综艺");
        set.add("综艺");
        set.add("综艺");
        set.add("综艺");
        set.add("母婴");
        return set;
    }
}