package com.cl.graph.weibo.data;

import com.cl.graph.weibo.data.entity.UserInfo;
import com.cl.graph.weibo.data.manager.RedisDataManager;
import com.cl.graph.weibo.data.util.CsvFileHelper;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author yejianyu
 * @date 2019/7/29
 */
public class TempDataTest {

    @Test
    public void test() throws IOException {
        String[] head1 = {"ID", "昵称", "类型", "领域标签1", "领域标签2", "领域标签3",
                "PGRank1", "PGRank2", "PGRank3", "PGRank4"};
        String[] head2 = {"ID", "昵称", "领域标签1", "领域标签2", "领域标签3",
                "PGRank1", "PGRank2", "PGRank3", "PGRank4"};

        String readFile = "C:/Users/cl32/Documents/weibo/weiboGraph/vertex/personal_important.csv";
        String resultFile = "C:/Users/cl32/Documents/weibo/weiboGraph/vertex1000w";

        try (CSVParser csvParser = CsvFileHelper.reader(readFile, head1, true);
             CSVPrinter yellowWriter = CsvFileHelper.writer(resultFile + "/yellow_v.csv", head2);
             CSVPrinter redWriter = CsvFileHelper.writer(resultFile + "/red_v.csv", head2);
             CSVPrinter showWriter = CsvFileHelper.writer(resultFile + "/show_v.csv", head2);
             CSVPrinter personalWriter = CsvFileHelper.writer(resultFile + "/personal.csv", head2)) {
            for (CSVRecord record : csvParser) {
                switch (record.get(2)) {
                    case "黄V":
                        writeToFile(yellowWriter, record);
                        break;
                    case "红V":
                        writeToFile(redWriter, record);
                        break;
                    case "达人":
                        writeToFile(showWriter, record);
                        break;
                    case "普通用户":
                        writeToFile(personalWriter, record);
                        break;
                    default:
                        break;
                }
            }
        }
    }

    @Test
    public void genFansFile() throws IOException {
        String[] head1 = {"ID", "昵称", "类型", "领域标签1", "领域标签2", "领域标签3",
                "PGRank1", "PGRank2", "PGRank3", "PGRank4"};
        String[] head2 = {"ID", "昵称", "领域标签1", "领域标签2", "领域标签3",
                "PGRank1", "PGRank2", "PGRank3", "PGRank4"};

        String readFile1 = "C:/Users/cl32/Documents/weibo/weiboGraph/vertex/personal_important.csv";
        Set<String> set = new HashSet<>();
        try (CSVParser csvParser = CsvFileHelper.reader(readFile1, head1, true)) {
            for (CSVRecord record : csvParser) {
                set.add(record.get(0));
            }
        }

        String readFile2 = "C:/Users/cl32/Documents/weibo/weiboGraph/vertex/personal_all.csv";
        String resultFile = "C:/Users/cl32/Documents/weibo/weiboGraph/vertex1000w";

        try (CSVParser csvParser = CsvFileHelper.reader(readFile2, head1, true);
             CSVPrinter fansWriter = CsvFileHelper.writer(resultFile + "/fans2.csv", head2)) {
            for (CSVRecord record : csvParser) {
                if (!set.contains(record.get(0))) {
                    writeToFile(fansWriter, record);
                }
            }
        }
    }

    private void writeToFile(CSVPrinter writer, CSVRecord record) throws IOException {
        writer.printRecord(record.get(0), record.get(1), record.get(3), record.get(4), record.get(5), record.get(6),
                record.get(7), record.get(8), record.get(9));
    }

    @Test
    public void addMcnSign() throws IOException {
        String[] head1 = {"ID", "昵称", "粉丝数", "关注数", "博文数", "领域标签1", "领域标签2", "领域标签3",
                "PGRank1", "PGRank2", "PGRank3", "PGRank4"};
        String[] head2 = {"ID", "昵称", "粉丝数", "关注数", "博文数", "领域标签1", "领域标签2", "领域标签3",
                "PGRank1", "PGRank2", "PGRank3", "PGRank4", "mcnSign"};

        String readFile1 = "C:/Users/cl32/Documents/weibo/weiboGraph/mcn.txt";
        Set<String> set = new HashSet<>();
        try (CSVParser csvParser = CsvFileHelper.reader(readFile1)) {
            for (CSVRecord record : csvParser) {
                set.add(record.get(0));
            }
        }

        String readFilePath = "C:/Users/cl32/Documents/weibo/weiboBigGraph/vertex";
        String resultFile = "C:/Users/cl32/Documents/weibo/weiboBigGraph/vertexMcn";
        File readFileDir = new File(readFilePath);
        File[] files = readFileDir.listFiles();
        for (File file : files) {
            try (CSVParser csvParser = CsvFileHelper.reader(file, head1, true);
                 CSVPrinter writer = CsvFileHelper.writer(resultFile + File.separator + file.getName(), head2)) {
                for (CSVRecord record : csvParser) {
                    if (set.contains(record.get(0))) {
                        writeMcnToFile(writer, record, "1");
                    } else {
                        writeMcnToFile(writer, record, "0");
                    }
                }
            }
        }
    }

    @Test
    public void getCoreUser() throws IOException {
        String[] head = {"ID", "昵称", "粉丝数", "关注数", "博文数", "领域标签1", "领域标签2", "领域标签3",
                "PGRank1", "PGRank2", "PGRank3", "PGRank4", "mcnSign"};

        String readFilePath = "C:/Users/cl32/Documents/weibo/weiboBigGraph/vertexMcn";

        String resultFile = "C:/Users/cl32/Documents/weibo/weiboBigGraph/vertex205w";

        File readFile = new File(readFilePath);
        File[] files = readFile.listFiles((dir, name) -> !"fans.csv".equals(name) && !"blue_v.csv".equals(name));
        for (File file : files) {
            try (CSVParser csvParser = CsvFileHelper.reader(file, head, true);
                 CSVPrinter writer = CsvFileHelper.writer(resultFile + File.separator + file.getName(), head)) {
                for (CSVRecord record : csvParser) {
                    String followers = record.get("粉丝数");
                    int followersCount = Integer.parseInt(followers);
                    if (followersCount >= 10000) {
                        writer.printRecord(record);
                    }
                }
            }
        }
    }

    private void writeMcnToFile(CSVPrinter writer, CSVRecord record, String mcnSign) throws IOException {
        writer.printRecord(record.get(0), record.get(1), record.get(2), record.get(3), record.get(4), record.get(5), record.get(6),
                record.get(7), record.get(8), record.get(9), record.get(10), record.get(11), mcnSign);
    }


    @Test
    public void genNonCoreUser() throws IOException {
        String[] head = {"ID", "昵称", "粉丝数", "关注数", "博文数", "领域标签1", "领域标签2", "领域标签3",
                "PGRank1", "PGRank2", "PGRank3", "PGRank4", "mcnSign"};

        String readFile1 = "C:/Users/cl32/Desktop/edge/nonCoreUser/showV.txt";
        Set<String> set = new HashSet<>();
        try (CSVParser csvParser = CsvFileHelper.reader(readFile1)) {
            for (CSVRecord record : csvParser) {
                set.add(record.get(0));
            }
        }

        String readFilePath = "C:/Users/cl32/Documents/weibo/weiboBigGraph/vertex/show_v.csv";
        String resultFile = "C:/Users/cl32/Desktop/edge/user";
        try (CSVParser csvParser = CsvFileHelper.reader(readFilePath, head, true);
             CSVPrinter writer = CsvFileHelper.writer(resultFile + File.separator + "non_core_show_v.csv", head)) {
            for (CSVRecord record : csvParser) {
                if (set.contains(record.get(0))) {
                    writer.printRecord(record);
                }
            }
        }
    }

    @Test
    public void test1() throws IOException {
        String[] head = {"from", "to"};
        Map<String, String> typeMap = new HashMap<>();
        typeMap.put("person", "KOL");
        typeMap.put("redV", "红V");
        typeMap.put("yellowV", "黄V");
        typeMap.put("blueV", "蓝V");
        typeMap.put("showV", "达人");
        typeMap.put("fans", "普通用户");
        String readDir1 = "C:/Users/cl32/Desktop/edge/test1";
        String readDir2 = "C:/Users/cl32/Desktop/edge/test2";
        File dir1 = new File(readDir1);
        File[] files = dir1.listFiles();
        for (File file : files) {
            String relation = StringUtils.replace(file.getName(), ".csv", "");
            String[] types = relation.split("_following_");
            String type1 = typeMap.get(types[0]);
            String type2 = typeMap.get(types[1]);
            String fileName = "follow_" + type1 + "_" + type2 + ".csv";

            try (CSVParser csvParser1 = CsvFileHelper.reader(file, head, true);
                 CSVParser csvParser2 = CsvFileHelper.reader(readDir2 + File.separator + fileName, head, true)) {
                Iterator<CSVRecord> iterator1 = csvParser1.iterator();
                Iterator<CSVRecord> iterator2 = csvParser2.iterator();
                while (iterator1.hasNext() && iterator2.hasNext()) {
                    CSVRecord record1 = iterator1.next();
                    CSVRecord record2 = iterator2.next();
                    if (!record1.get(0).equals(record2.get(0)) || !record1.get(1).equals(record2.get(1))) {
                        System.out.println(record1.get(0) + "  " + record1.get(1));
                        System.out.println(record2.get(0) + "  " + record2.get(1));
                        System.out.println("关系不同:" + fileName);
                    }
                }
                if (iterator1.hasNext() || iterator2.hasNext()) {
                    System.out.println("边数不同：" + fileName);
                }
            }
        }
    }

    @Test
    public void test2() throws IOException {
        String[] head = new String[]{"index", "text"};
        Lock lock = new ReentrantLock();
        for (int i = 0; i < 100; i++) {
            int finalI = i;
            new Thread(() -> {
                File file = new File("C:/Users/cl32/Desktop/test/test.csv");
                lock.lock();
                boolean append = file.exists();
                try (CSVPrinter csvPrinter = CsvFileHelper.writer("C:/Users/cl32/Desktop/test/test.csv", head, append)) {
                    csvPrinter.printRecord(finalI, "test," + finalI);
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    lock.unlock();
                }
            }).start();
        }
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test11() {
        StringBuilder str = new StringBuilder();
        int length = RandomUtils.nextInt(2, 20);
        for (int i = 0; i < length; i++) {
            char c = (char) (0x4e00 + (int) (Math.random() * (0x9fa5 - 0x4e00 + 1)));
            str.append(c);
        }

        System.out.print(str);
    }
}
