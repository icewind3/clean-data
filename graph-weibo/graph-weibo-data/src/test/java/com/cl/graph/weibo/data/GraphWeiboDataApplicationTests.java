package com.cl.graph.weibo.data;

import com.cl.graph.weibo.data.constant.SSDBConstants;
import com.cl.graph.weibo.data.util.CsvFileHelper;
import com.cl.graph.weibo.data.util.DateTimeUtils;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.time.DateUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(SpringRunner.class)
@SpringBootTest
public class GraphWeiboDataApplicationTests {

    @Resource(name = "userFriendsFollowersRedisTemplate")
    private StringRedisTemplate userFriendsFollowersRedisTemplate;

    @Resource(name = "userInfoRedisTemplate")
    private StringRedisTemplate userInfoRedisTemplate;

    @Test
    public void contextLoads() {
        String key = "show_v_friends_followers_lt_1w";
        ScanOptions scanOptions = ScanOptions.scanOptions().count(1000).match("*").build();
        AtomicInteger count = new AtomicInteger();
//        try (Cursor<Map.Entry<Object, Object>> cursor = userFriendsFollowersRedisTemplate.opsForHash().scan(key, scanOptions)){
//            cursor.forEachRemaining(entry -> {
//                Object key1 = entry.getKey();
//                System.out.println(entry);
//                count.getAndIncrement();
//            });
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        userFriendsFollowersRedisTemplate.execute((RedisConnection connection) -> {
            try (Cursor<byte[]> cursor = connection.scan(ScanOptions.scanOptions().count(Long.MAX_VALUE).match("*").build())) {
                cursor.forEachRemaining(item -> {
                    String s = new String(item, StandardCharsets.UTF_8);
                    System.out.println(s);
                    count.getAndIncrement();
                });
                return null;
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
        System.out.println(count.get());
    }

    @Test
    public void test() throws ParseException {
        String startDate = "20190501";
        String endDate = "20190503";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        Date date1 = simpleDateFormat.parse(startDate);
        Date date2 = simpleDateFormat.parse(endDate);
        while (!date1.after(date2)) {
            System.out.println(simpleDateFormat.format(date1));
            date1 = DateUtils.addDays(date1, 1);
        }
    }

    @Test
    public void test2() throws IOException {
        String[] header = new String[]{"from", "to", "mid", "createTime"};

        Set<String> set = new HashSet<>();
        try (CSVParser reader = CsvFileHelper.reader("C:/Users/cl32/Documents/weibo/微博大图/vertex/blue_v.csv", new String[]{}, true);) {
            for (CSVRecord record : reader) {
                set.add(record.get(0));
            }
        }
        try (CSVParser reader = CsvFileHelper.reader("C:/Users/cl32/Documents/weibo/微博大图/vertex/personal_core.csv", new String[]{}, true)) {
            for (CSVRecord record : reader) {
                set.add(record.get(0));
            }
        }
        String fileDirPath = "C:/Users/cl32/Documents/weibo/微博大图/edge";
        File fileDir = new File(fileDirPath);
        File[] files = fileDir.listFiles();
        if (files == null || files.length == 0) {
            System.out.println("文件夹为空");
            return;
        }
        String resultPath = "C:/Users/cl32/Documents/weibo/微博大图/edgeFilter";
        for (File file : files) {
            try (CSVParser reader = CsvFileHelper.reader(file, header, true);
                 CSVPrinter csvPrinter = CsvFileHelper.writer(new File(resultPath + File.separator + file.getName()), header)) {
                for (CSVRecord record : reader) {
                    if (set.contains(record.get(0))) {
                        csvPrinter.printRecord(record);
                    }
                }
            }
        }
    }

    @Test
    public void test3() throws IOException {
        String resultPath = "C:/Users/cl32/Documents/weibo/weiboBigGraph/uid";
//        String readFile = "blue_v.csv";
//        String writeFile = "uid_blue_v.csv";
//        String readFile = "personal_core.csv";
//        String writeFile = "uid_personal_core.csv";
//        String readFile = "personal_important.csv";
//        String writeFile = "uid_personal_important.csv";
//        String readFile = "personal_all.csv";
//        String writeFile = "uid_personal_all.csv";
        String readDir = "C:/Users/cl32/Documents/weibo/weiboBigGraph/vertex/fans.csv";
        String writeFile = "uid_fans.csv";
        File readFile = new File(readDir);
        try (CSVPrinter csvPrinter = CsvFileHelper.writer(new File(resultPath + File.separator + writeFile))) {
            try (CSVParser reader = CsvFileHelper.reader(readFile, new String[]{}, true)) {
                for (CSVRecord record : reader) {
                    csvPrinter.printRecord(record.get(0));
                }
            }
        }
    }


}
