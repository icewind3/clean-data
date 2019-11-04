package com.cl.data.file;

import com.cl.graph.weibo.core.util.CsvFileHelper;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

/**
 * @author yejianyu
 * @date 2019/10/9
 */
public class FileFilterTest {

    @Test
    public void test() throws IOException {
        String uidPath = "C:\\Users\\cl32\\Documents\\weibo\\weiboBigGraphNew\\uid_core_new/uid_80w.csv";
        Set<String> uidSet = getSet(uidPath);


        String filePath = "C:\\Users\\cl32\\Documents\\weibo\\weiboBigGraphNew\\uid_core_new";
        String resultPath = "C:\\Users\\cl32\\Documents\\weibo\\weiboBigGraphNew\\uid_core_new_80w";
        File file = new File(filePath);
        File[] files = file.listFiles();
        for (File file1 : files) {
            try (CSVPrinter csvPrinter = CsvFileHelper.writer(resultPath + File.separator + file1.getName())) {
                traverseFile(file1, record -> {
                    if (uidSet.contains(record.get(0))){
                        try {
                            csvPrinter.printRecord(record);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        }
    }

    private Set<String> getSet(String filePath) throws IOException {
        Set<String> set = new HashSet<>();
        try (CSVParser csvParser = CsvFileHelper.reader(filePath)){
            for (CSVRecord record : csvParser) {
                set.add(record.get(0));
            }
        }
        return set;
    }

    private void traverseFile(File file, Consumer<CSVRecord> consumer) throws IOException {
        try (CSVParser csvParser = CsvFileHelper.reader(file)){
            for (CSVRecord record : csvParser) {
                consumer.accept(record);
            }
        }
    }

    @Test
    public void test2() throws IOException {
        String uidPath = "C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\已爬完博文/20190911_fans_mblog.txt";
        String uidPath2 = "C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\已爬完博文/20190912_fans_mblog_3.txt";
        Set<String> uidSet = getSet(uidPath);
        Set<String> uidSet2 = getSet(uidPath2);
        uidSet.addAll(uidSet2);


        String filePath = "C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\已爬完博文/20190912_fans_mblog_all.txt";
        String resultPath = "C:\\Users\\cl32\\Documents\\weibo\\订单预处理\\已爬完博文/20190912_fans_mblog_new.txt";
        try (CSVPrinter csvPrinter = CsvFileHelper.writer(resultPath)) {
            traverseFile(new File(filePath), record -> {
                if (!uidSet.contains(record.get(0))){
                    try {
                        csvPrinter.printRecord(record);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    @Test
    public void test4() throws IOException {
        String filePath = "C:\\Users\\cl32\\Downloads\\user_blog_stat_all.csv";
        String resultPath = "C:\\Users\\cl32\\Downloads\\user_blog_stat_220w_20191022.csv";
        String[] header = {"uid", "blog_total", "retweet_blog_count", "blog_frequency", "original_blog_frequency",
            "original_ratio"};
        try (CSVPrinter csvPrinter = CsvFileHelper.writer(resultPath, header)) {
            traverseFile(new File(filePath), record -> {
                try {
                    csvPrinter.printRecord(record.get(0),record.get(1),record.get(2),record.get(5),record.get(6),
                        record.get(7));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    @Test
    public void test5() throws IOException {
        String filePath = "C:\\Users\\cl32\\Downloads\\user_blog_stat_all.csv";
        String uidPath = "C:\\Users\\cl32\\Documents\\weibo\\weiboBigGraphNew\\uid_core_new/uid_220w.csv";
        String resultPath = "C:\\Users\\cl32\\Downloads\\user_blog_stat_all_20191022_new.csv";
//        String[] header = {"uid", "blog_total", "retweet_blog_count", "blog_frequency", "original_blog_frequency",
//            "original_ratio"};
//        String[] header = {"uid", "last_time", "blog_frequency_week", "retweet_ratio"};
        String[] header = {"uid", "blog_frequency_week", "retweet_ratio", "last_time"};
//        Set<String> uidSet = CsvFileHelper.getStringHashSet(uidPath);
        try (CSVPrinter csvPrinter = CsvFileHelper.writer(resultPath, header)) {
            traverseFile(new File(filePath), record -> {
                try {
                    String uid = record.get(0);
//                    if (uidSet.contains(uid)){
                        int blogTotal = Integer.parseInt(record.get(1));
                        int retweetTotal = Integer.parseInt(record.get(2));
                        csvPrinter.printRecord(uid, record.get(5), (float) retweetTotal / blogTotal,
                            record.get(4));
//                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    @Test
    public void test6() throws IOException {
        String filePath = "C:\\Users\\cl32\\Desktop\\水军/zombie_mblog_percent.txt";
        String uidPath = "C:\\Users\\cl32\\Documents\\weibo\\weiboBigGraphNew\\uid_core_new/uid_80w.csv";
        String resultPath = "C:\\Users\\cl32\\Desktop\\水军/kol_80w_zombie_mblog_gte_60.txt";
        Set<String> uidSet = CsvFileHelper.getStringHashSet(uidPath);
        try (CSVPrinter csvPrinter = CsvFileHelper.writer(resultPath)) {
            traverseFile(new File(filePath), record -> {
                try {
                    String uid = record.get(0);
                    if (uidSet.contains(uid)){
                        int blogCount = Integer.parseInt(record.get(1));
                        if (blogCount >=60) {
                            csvPrinter.printRecord(record);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

}
