package com.cl.data.file.service;

import com.cl.graph.weibo.core.util.CsvFileHelper;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/9/12
 */
public class UserBlogStatFilterServiceTest {

    private static final String[] HEADER_USER_BLOG_STAT = {"uid", "releaseFrequency", "attitudeAvg", "commentAvg", "repostAvg",
        "attitudeMedian", "commentMedian", "repostMedian", "repostRate", "releaseFrequency2", "attitudeAvg2",
        "commentAvg2", "repostAvg2", "attitudeMedian2", "commentMedian2", "repostMedian2", "repostRate2", "attitudeTopAvg",
        "commentTopAvg", "repostTopAvg"};

    private static final String[] HEADER_USER_BLOG_STAT_TOP = {"uid", "attitudeTopAvg", "commentTopAvg", "repostTopAvg"};


    @Test
    public void filterUser() {

        try (CSVParser csvParser = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/user_blog_stat_220w.csv", HEADER_USER_BLOG_STAT, true);
             CSVPrinter csvPrinter = CsvFileHelper.writer("C:/Users/cl32/Desktop/stat/user_blog_stat_220w_inactivity.csv", HEADER_USER_BLOG_STAT)) {
            for (CSVRecord record : csvParser) {
//                if (Float.parseFloat(record.get("releaseFrequency")) < 250) {
//                    continue;
//                }
//                if (Float.parseFloat(record.get("releaseFrequency2")) < 250) {
//                    continue;
//                }
                float releaseFrequency = Float.parseFloat(record.get("releaseFrequency"));
//                float releaseFrequency2 = Float.parseFloat(record.get("releaseFrequency2"));
//                if ((releaseFrequency >= 5 && releaseFrequency < 20) || (releaseFrequency2 >= 5 && releaseFrequency2 < 20)) {
//                    csvPrinter.printRecord(record);
//                }
                if (releaseFrequency == 0 ) {
                    csvPrinter.printRecord(record);
                }
//                int sum = Integer.parseInt(record.get("attitudeAvg")) + Integer.parseInt(record.get("commentAvg"))
//                    + Integer.parseInt(record.get("repostAvg"));
//                int sum2 = Integer.parseInt(record.get("attitudeAvg2")) + Integer.parseInt(record.get("commentAvg2"))
//                    + Integer.parseInt(record.get("repostAvg2"));
//                if ( sum < 10 && sum2 < 10){
//                    csvPrinter.printRecord(record);
//                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void filter() {

        Set<String> uidSet = new HashSet<>();
//        try (CSVParser csvParser1 = CsvFileHelper.reader("C:/Users/cl32/Documents/weibo/weiboBigGraph/uid/uid_personal_core.csv");
//             CSVParser csvParser2 = CsvFileHelper.reader("C:/Users/cl32/Documents/weibo/weiboBigGraph/uid/uid_blue_v.csv")) {
        try (CSVParser csvParser2 = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/uid/uid_blue_mt.csv")) {
//            for (CSVRecord record : csvParser1) {
//                uidSet.add(record.get(0));
//            }
            for (CSVRecord record : csvParser2) {
                uidSet.add(record.get(0));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        float max1 = 0;
        float max2 = 0;
        float min1 = 200;
        float min2 = 200;
        try (CSVParser csvParser = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/user_blog_stat.csv", HEADER_USER_BLOG_STAT);
             CSVPrinter csvPrinter = CsvFileHelper.writer("C:/Users/cl32/Desktop/stat/user_blog_stat_blue_mt.csv", HEADER_USER_BLOG_STAT)) {

            for (CSVRecord record : csvParser) {
                String uid = record.get(0);
                if (uidSet.contains(uid)) {
                    float releaseFrequency = Float.parseFloat(record.get("releaseFrequency"));
                    float releaseFrequency2 = Float.parseFloat(record.get("releaseFrequency2"));
                    max1 = Math.max(max1, releaseFrequency);
                    max2 = Math.max(max2, releaseFrequency2);
                    min1 = Math.min(min1, releaseFrequency);
                    min2 = Math.min(min1, releaseFrequency2);
                    if (releaseFrequency > 5) {
                        csvPrinter.printRecord(record);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(max1);
        System.out.println(max2);
        System.out.println(min1);
        System.out.println(min2);
    }

    @Test
    public void test() {
        Set<String> uidSet = new HashSet<>();
//        try (CSVParser csvParser1 = CsvFileHelper.reader("C:/Users/cl32/Documents/weibo/weiboBigGraphNew/uid_core_new/uid_220w.csv")) {
        try (CSVParser csvParser1 = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/user_filter_name2.csv", new String[]{}, true)) {
            for (CSVRecord record : csvParser1) {
                uidSet.add(record.get(0));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try (CSVParser csvParser = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/user_blog_stat_220w_gte20_3.csv", HEADER_USER_BLOG_STAT, true);
             CSVPrinter csvPrinter = CsvFileHelper.writer("C:/Users/cl32/Desktop/stat/user_blog_stat_220w_suspected_2.csv", HEADER_USER_BLOG_STAT)) {
            for (CSVRecord record : csvParser) {
                if (!uidSet.contains(record.get(0))) {
                    csvPrinter.printRecord(record);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void filterUser2() {

        String type = "personal";
        try (CSVParser csvParser = CsvFileHelper.reader("C:/Users/cl32/Documents/weibo/weiboBigGraph/vertex/" + type + ".csv", new String[]{}, true);
             CSVPrinter writer1 = CsvFileHelper.writer("C:/Users/cl32/Desktop/stat/uid/" + type + "_lt_1w.csv");
             CSVPrinter writer2 = CsvFileHelper.writer("C:/Users/cl32/Desktop/stat/uid/" + type + "_1w_3w.csv");
             CSVPrinter writer3 = CsvFileHelper.writer("C:/Users/cl32/Desktop/stat/uid/" + type + "_3w_10w.csv");
             CSVPrinter writer4 = CsvFileHelper.writer("C:/Users/cl32/Desktop/stat/uid/" + type + "_10w_50w.csv");
             CSVPrinter writer5 = CsvFileHelper.writer("C:/Users/cl32/Desktop/stat/uid/" + type + "_50w_100w.csv");
             CSVPrinter writer6 = CsvFileHelper.writer("C:/Users/cl32/Desktop/stat/uid/" + type + "_gte_100w.csv")) {
            for (CSVRecord record : csvParser) {
                String uid = record.get(0);
                int fans = Integer.parseInt(record.get(2));
                if (fans < 10000) {
                    writer1.printRecord(uid);
                } else if (fans < 30000) {
                    writer2.printRecord(uid);
                } else if (fans < 100000) {
                    writer3.printRecord(uid);
                } else if (fans < 500000) {
                    writer4.printRecord(uid);
                } else if (fans < 1000000) {
                    writer5.printRecord(uid);
                } else {
                    writer6.printRecord(uid);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void filterUser3() {

        //        Set<String> uidSet1 = new HashSet<>();
//        try (CSVParser csvParser1 = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/uid/" + file)) {
//            for (CSVRecord record : csvParser1) {
//                uidSet1.add(record.get(0));
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        String file = "C:/Users/cl32/Documents/weibo/weiboBigGraph/uid205w/uid_personal.csv";
        String file = "C:/Users/cl32/Desktop/stat/uid/uid_blue_qiye.csv";
        Set<String> uidSet2 = new HashSet<>();
        try (CSVParser csvParser2 = CsvFileHelper.reader(file)) {
            for (CSVRecord record : csvParser2) {
                String uid = record.get(0);
                uidSet2.add(uid);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        String type = "blue_v";
        Set<String> uidSet = new HashSet<>();
        try (CSVParser reader1 = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/uid/" + type + "_lt_1w.csv");
             CSVParser reader2 = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/uid/" + type + "_1w_3w.csv");
             CSVParser reader3 = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/uid/" + type + "_3w_10w.csv");
             CSVParser reader4 = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/uid/" + type + "_10w_50w.csv");
             CSVParser reader5 = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/uid/" + type + "_50w_100w.csv");
             CSVParser reader6 = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/uid/" + type + "_gte_100w.csv")) {
            for (CSVRecord record : reader1) {
                String uid = record.get(0);
                if (uidSet2.contains(uid)) {
                    uidSet.add(uid);
                }
            }
            for (CSVRecord record : reader2) {
                String uid = record.get(0);
                if (uidSet2.contains(uid)) {
                    uidSet.add(uid);
                }
            }
            for (CSVRecord record : reader3) {
                String uid = record.get(0);
                if (uidSet2.contains(uid)) {
                    uidSet.add(uid);
                }
            }
            for (CSVRecord record : reader4) {
                String uid = record.get(0);
                if (uidSet2.contains(uid)) {
                    uidSet.add(uid);
                }
            }
            for (CSVRecord record : reader5) {
                String uid = record.get(0);
                if (uidSet2.contains(uid)) {
                    uidSet.add(uid);
                }
            }
            for (CSVRecord record : reader6) {
                String uid = record.get(0);
                if (uidSet2.contains(uid)) {
                    uidSet.add(uid);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        int count = 0;
        try (CSVParser csvParser = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/user_blog_stat_205w.csv", HEADER_USER_BLOG_STAT, true);
             CSVPrinter csvPrinter = CsvFileHelper.writer("C:/Users/cl32/Desktop/stat/uid_suspected/企业.txt")) {
            for (CSVRecord record : csvParser) {
                String uid = record.get(0);
                if (uidSet.contains(uid)) {
                    float releaseFrequency = Float.parseFloat(record.get("releaseFrequency"));
                    float releaseFrequency2 = Float.parseFloat(record.get("releaseFrequency2"));
                    if (releaseFrequency >= 100 || releaseFrequency2 >= 100) {
                        csvPrinter.printRecord(uid);
                        count++;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(count);
    }


    @Test
    public void countStat() {

        Set<String> uidSet = new HashSet<>();
//        try (CSVParser csvParser1 = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/uid_yhq/优惠券.txt")) {
//            for (CSVRecord record : csvParser1) {
//                uidSet.add(record.get(0));
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        try (CSVParser csvParser2 = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/uid_yhq/优惠券(1).txt")) {
//            for (CSVRecord record : csvParser2) {
//                uidSet.add(record.get(0));
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        try (CSVParser csvParser2 = CsvFileHelper.reader("C:\\Users\\cl32\\Documents\\weibo\\weiboBigGraphNew\\uid_delete/uid_core_delete2.csv")) {
            for (CSVRecord record : csvParser2) {
                uidSet.add(record.get(0));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        float max1 = 0;
        float max2 = 0;
        float min1 = 10;
        float min2 = 10;
        int count = 0;
        float sum1 = 0;
        float sum2 = 0;
        int attitudeSum1 = 0;
        int commentSum1 = 0;
        int repostSum1 = 0;
        int attitudeSum2 = 0;
        int commentSum2 = 0;
        int repostSum2 = 0;

        float releaseFrequencyMax1 = 0;
        float releaseFrequencyMax2 = 0;
        float releaseFrequencyMin1 = 10000;
        float releaseFrequencyMin2 = 10000;
        float releaseFrequencySum1 = 0;
        float releaseFrequencySum2 = 0;
        try (CSVParser csvParser = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/user_blog_stat_205w.csv", HEADER_USER_BLOG_STAT)) {
            for (CSVRecord record : csvParser) {
                String uid = record.get(0);
                if (uidSet.contains(uid)) {
                    float value1 = Float.parseFloat(record.get("repostRate"));
                    float value2 = Float.parseFloat(record.get("repostRate2"));
                    sum1 += value1;
                    sum2 += value2;
                    max1 = Math.max(max1, value1);
                    max2 = Math.max(max2, value2);
                    min1 = Math.min(min1, value1);
                    min2 = Math.min(min2, value2);
                    attitudeSum1 += Integer.parseInt(record.get("attitudeAvg"));
                    commentSum1 += Integer.parseInt(record.get("commentAvg"));
                    repostSum1 += Integer.parseInt(record.get("repostAvg"));
                    attitudeSum2 += Integer.parseInt(record.get("attitudeAvg2"));
                    commentSum2 += Integer.parseInt(record.get("commentAvg2"));
                    repostSum2 += Integer.parseInt(record.get("repostAvg2"));
                    count++;

                    float releaseFrequency = Float.parseFloat(record.get("releaseFrequency"));
                    float releaseFrequency2 = Float.parseFloat(record.get("releaseFrequency2"));
                    releaseFrequencySum1 += releaseFrequency;
                    releaseFrequencySum2 += releaseFrequency2;
                    releaseFrequencyMax1 = Math.max(releaseFrequencyMax1, releaseFrequency);
                    releaseFrequencyMax2 = Math.max(releaseFrequencyMax2, releaseFrequency2);
                    releaseFrequencyMin1 = Math.min(releaseFrequencyMin1, releaseFrequency);
                    releaseFrequencyMin2 = Math.min(releaseFrequencyMin2, releaseFrequency2);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(String.format("%f\t%f\t%f\t%d\t%d\t%d\t%f\t%f\t%f", sum1 / count, min1, max1, attitudeSum1 / count,
            commentSum1 / count, repostSum1 / count, releaseFrequencySum1 / count, releaseFrequencyMin1, releaseFrequencyMax1));
        System.out.println(String.format("%f\t%f\t%f\t%d\t%d\t%d\t%f\t%f\t%f", sum2 / count, min2, max2, attitudeSum2 / count,
            commentSum2 / count, repostSum2 / count, releaseFrequencySum2 / count, releaseFrequencyMin2, releaseFrequencyMax2));
        System.out.println(count);
    }

    @Test
    public void countBlueStat() {

        Set<String> uidSet1 = new HashSet<>();
        Set<String> uidSet2 = new HashSet<>();
        try (CSVParser csvParser1 = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/uid/blue_v_1w_3w.csv")) {
            for (CSVRecord record : csvParser1) {
                uidSet1.add(record.get(0));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (CSVParser csvParser2 = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/uid/uid_blue_dsqy.csv")) {
            for (CSVRecord record : csvParser2) {
                String uid = record.get(0);
                if (uidSet1.contains(uid)) {
                    uidSet2.add(uid);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        float max1 = 0;
        float max2 = 0;
        float min1 = 10;
        float min2 = 10;
        int count = 0;
        float sum1 = 0;
        float sum2 = 0;
        int attitudeSum1 = 0;
        int commentSum1 = 0;
        int repostSum1 = 0;
        int attitudeSum2 = 0;
        int commentSum2 = 0;
        int repostSum2 = 0;


        float releaseFrequencyMax1 = 0;
        float releaseFrequencyMax2 = 0;
        float releaseFrequencyMin1 = 10000;
        float releaseFrequencyMin2 = 10000;
        float releaseFrequencySum1 = 0;
        float releaseFrequencySum2 = 0;
        try (CSVParser csvParser = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/user_blog_stat_205w.csv", HEADER_USER_BLOG_STAT, true)) {
            for (CSVRecord record : csvParser) {
                String uid = record.get(0);
                if (uidSet2.contains(uid)) {
                    float value1 = Float.parseFloat(record.get("repostRate"));
                    float value2 = Float.parseFloat(record.get("repostRate2"));
                    sum1 += value1;
                    sum2 += value2;
                    max1 = Math.max(max1, value1);
                    max2 = Math.max(max2, value2);
                    min1 = Math.min(min1, value1);
                    min2 = Math.min(min2, value2);
                    attitudeSum1 += Integer.parseInt(record.get("attitudeAvg"));
                    commentSum1 += Integer.parseInt(record.get("commentAvg"));
                    repostSum1 += Integer.parseInt(record.get("repostAvg"));
                    attitudeSum2 += Integer.parseInt(record.get("attitudeAvg2"));
                    commentSum2 += Integer.parseInt(record.get("commentAvg2"));
                    repostSum2 += Integer.parseInt(record.get("repostAvg2"));
                    count++;

                    float releaseFrequency = Float.parseFloat(record.get("releaseFrequency"));
                    float releaseFrequency2 = Float.parseFloat(record.get("releaseFrequency2"));
                    releaseFrequencySum1 += releaseFrequency;
                    releaseFrequencySum2 += releaseFrequency2;
                    releaseFrequencyMax1 = Math.max(releaseFrequencyMax1, releaseFrequency);
                    releaseFrequencyMax2 = Math.max(releaseFrequencyMax2, releaseFrequency2);
                    releaseFrequencyMin1 = Math.min(releaseFrequencyMin1, releaseFrequency);
                    releaseFrequencyMin2 = Math.min(releaseFrequencyMin2, releaseFrequency2);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (count == 0) {
            System.out.println(count);
            return;
        }
        System.out.println(String.format("%f\t%f\t%f\t%d\t%d\t%d\t%f\t%f\t%f", sum1 / count, min1, max1, attitudeSum1 / count,
            commentSum1 / count, repostSum1 / count, releaseFrequencySum1 / count, releaseFrequencyMin1, releaseFrequencyMax1));
        System.out.println(String.format("%f\t%f\t%f\t%d\t%d\t%d\t%f\t%f\t%f", sum2 / count, min2, max2, attitudeSum2 / count,
            commentSum2 / count, repostSum2 / count, releaseFrequencySum2 / count, releaseFrequencyMin2, releaseFrequencyMax2));
        System.out.println(count);
    }


    @Test
    public void countBlueStatPercentage() {
        String type = "blue_v";
        countBlueStatPercentage(type + "_1w_3w.csv");
        countBlueStatPercentage(type + "_3w_10w.csv");
        countBlueStatPercentage(type + "_10w_50w.csv");
        countBlueStatPercentage(type + "_50w_100w.csv");
        countBlueStatPercentage(type + "_gte_100w.csv");

    }


    private void countBlueStatPercentage(String file) {

        Set<String> uidSet1 = new HashSet<>();
        Set<String> uidSet2 = new HashSet<>();
        try (CSVParser csvParser1 = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/uid/" + file)) {
            for (CSVRecord record : csvParser1) {
                uidSet2.add(record.get(0));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (CSVParser csvParser2 = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/uid/uid_blue_qiye.csv")) {
            for (CSVRecord record : csvParser2) {
                String uid = record.get(0);
                if (uidSet2.contains(uid)) {
                    uidSet1.add(uid);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<Float> list1 = new ArrayList<>();
        List<Float> list2 = new ArrayList<>();

        try (CSVParser csvParser = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/user_blog_stat_205w.csv", HEADER_USER_BLOG_STAT, true)) {
            for (CSVRecord record : csvParser) {
                String uid = record.get(0);
                if (uidSet1.contains(uid)) {
                    float releaseFrequency = Float.parseFloat(record.get("releaseFrequency"));
                    float releaseFrequency2 = Float.parseFloat(record.get("releaseFrequency2"));
                    list1.add(releaseFrequency);
                    list2.add(releaseFrequency2);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        Collections.sort(list1);
        Collections.sort(list2);
        int size1 = list1.size();
        int size2 = list2.size();
        Float p1 = list1.get((int) (size1 * 0.993));
        Float p2 = list1.get((int) (size1 * 0.995));
        Float p3 = list1.get((int) (size1 * 0.996));
        Float p4 = list1.get((int) (size1 * 0.997));
        Float p5 = list1.get((int) (size1 * 0.998));
        Float p6 = list1.get((int) (size1 * 0.9995));


        Float p7 = list2.get((int) (size2 * 0.993));
        Float p8 = list2.get((int) (size2 * 0.995));
        Float p9 = list2.get((int) (size2 * 0.996));
        Float p10 = list2.get((int) (size2 * 0.997));
        Float p11 = list2.get((int) (size2 * 0.998));
        Float p12 = list2.get((int) (size2 * 0.9995));
        System.out.println(String.format("%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f", p1, p2, p3, p4, p5, p6,
            p7, p8, p9, p10, p11, p12));


//        System.out.println(String.format("%f\t%f\t%f\t%d\t%d\t%d\t%f\t%f\t%f", sum1 / count, min1, max1, attitudeSum1 / count,
//            commentSum1 / count, repostSum1 / count, releaseFrequencySum1 / count, releaseFrequencyMin1, releaseFrequencyMax1));
//        System.out.println(String.format("%f\t%f\t%f\t%d\t%d\t%d\t%f\t%f\t%f", sum2 / count, min2, max2, attitudeSum2 / count,
//            commentSum2 / count, repostSum2 / count, releaseFrequencySum2 / count, releaseFrequencyMin2, releaseFrequencyMax2));
//        System.out.println(count);
    }


    @Test
    public void countStatPercentage() {
        String type = "blue_v";
        countStatPercentage(type + "_1w_3w.csv");
        countStatPercentage(type + "_3w_10w.csv");
        countStatPercentage(type + "_10w_50w.csv");
        countStatPercentage(type + "_50w_100w.csv");
        countStatPercentage(type + "_gte_100w.csv");

    }

    private void countStatPercentage(String file) {

        Set<String> uidSet1 = new HashSet<>();
        try (CSVParser csvParser1 = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/uid/" + file)) {
            for (CSVRecord record : csvParser1) {
                uidSet1.add(record.get(0));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<Float> list1 = new ArrayList<>();
        List<Float> list2 = new ArrayList<>();

        try (CSVParser csvParser = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/user_blog_stat_205w.csv", HEADER_USER_BLOG_STAT, true)) {
            for (CSVRecord record : csvParser) {
                String uid = record.get(0);
                if (uidSet1.contains(uid)) {
                    float releaseFrequency = Float.parseFloat(record.get("releaseFrequency"));
                    float releaseFrequency2 = Float.parseFloat(record.get("releaseFrequency2"));
                    list1.add(releaseFrequency);
                    list2.add(releaseFrequency2);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        Collections.sort(list1);
        Collections.sort(list2);
        int size1 = list1.size();
        int size2 = list2.size();
        Float p1 = list1.get((int) (size1 * 0.96));
        Float p2 = list1.get((int) (size1 * 0.97));
        Float p3 = list1.get((int) (size1 * 0.98));
        Float p4 = list1.get((int) (size1 * 0.985));


        Float p5 = list2.get((int) (size2 * 0.96));
        Float p6 = list2.get((int) (size2 * 0.97));
        Float p7 = list2.get((int) (size2 * 0.98));
        Float p8 = list2.get((int) (size2 * 0.985));
        System.out.println(String.format("%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f", p1, p2, p3, p4, p5, p6, p7, p8));


//        System.out.println(String.format("%f\t%f\t%f\t%d\t%d\t%d\t%f\t%f\t%f", sum1 / count, min1, max1, attitudeSum1 / count,
//            commentSum1 / count, repostSum1 / count, releaseFrequencySum1 / count, releaseFrequencyMin1, releaseFrequencyMax1));
//        System.out.println(String.format("%f\t%f\t%f\t%d\t%d\t%d\t%f\t%f\t%f", sum2 / count, min2, max2, attitudeSum2 / count,
//            commentSum2 / count, repostSum2 / count, releaseFrequencySum2 / count, releaseFrequencyMin2, releaseFrequencyMax2));
//        System.out.println(count);
    }


    @Test
    public void countStatGte100() {
        String type = "blue_v";
        countStatGte100(type + "_1w_3w.csv");
        countStatGte100(type + "_3w_10w.csv");
        countStatGte100(type + "_10w_50w.csv");
        countStatGte100(type + "_50w_100w.csv");
        countStatGte100(type + "_gte_100w.csv");

    }

    private void countStatGte100(String file) {

//        Set<String> uidSet1 = new HashSet<>();
//        try (CSVParser csvParser1 = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/uid/" + file)) {
//            for (CSVRecord record : csvParser1) {
//                uidSet1.add(record.get(0));
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        Set<String> uidSet1 = new HashSet<>();
        Set<String> uidSet2 = new HashSet<>();
        try (CSVParser csvParser1 = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/uid/" + file)) {
            for (CSVRecord record : csvParser1) {
                uidSet2.add(record.get(0));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (CSVParser csvParser2 = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/uid/uid_blue_qiye.csv")) {
            for (CSVRecord record : csvParser2) {
                String uid = record.get(0);
                if (uidSet2.contains(uid)) {
                    uidSet1.add(uid);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        int count1 = 0;
        int count2 = 0;
        try (CSVParser csvParser = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/user_blog_stat_205w.csv", HEADER_USER_BLOG_STAT, true)) {
            for (CSVRecord record : csvParser) {
                String uid = record.get(0);
                if (uidSet1.contains(uid)) {
                    float releaseFrequency = Float.parseFloat(record.get("releaseFrequency"));
                    float releaseFrequency2 = Float.parseFloat(record.get("releaseFrequency2"));
                    if (releaseFrequency >= 100) {
                        count1++;
                    }
                    if (releaseFrequency2 >= 100) {
                        count2++;
                    }

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(String.format("%d\t%d", count1, count2));
    }

    @Test
    public void filterUser4() {

        int count1 = 0;
        int count2 = 0;
        int count3 = 0;
//
//        try (CSVParser csvParser = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/user_blog_stat_220w.csv", HEADER_USER_BLOG_STAT, true);
//             CSVPrinter csvPrinter = CsvFileHelper.writer("C:/Users/cl32/Desktop/stat/user_blog_stat_220w_gte20_3.csv", HEADER_USER_BLOG_STAT)) {
        try (CSVParser csvParser = CsvFileHelper.reader("C:\\Users\\cl32\\Desktop\\user_youhuiquan/user_blog_stat_car.csv", HEADER_USER_BLOG_STAT, true);
             CSVPrinter csvPrinter = CsvFileHelper.writer("C:\\Users\\cl32\\Desktop\\user_youhuiquan/user_blog_stat_car_gte50.csv", HEADER_USER_BLOG_STAT)) {
            for (CSVRecord record : csvParser) {
//                String uid = record.get(0);
                int sum1 = 0;
                int sum2 = 0;
                float releaseFrequency = Float.parseFloat(record.get("releaseFrequency"));
                float releaseFrequency2 = Float.parseFloat(record.get("releaseFrequency2"));
                sum1 += Integer.parseInt(record.get("attitudeAvg"));
                sum1 += Integer.parseInt(record.get("commentAvg"));
                sum1 += Integer.parseInt(record.get("repostAvg"));
                sum2 += Integer.parseInt(record.get("attitudeAvg2"));
                sum2 += Integer.parseInt(record.get("commentAvg2"));
                sum2 += Integer.parseInt(record.get("repostAvg2"));
//                if (releaseFrequency >= 20) {
//                    count1++;
//                }
//                if (releaseFrequency2 >= 30) {
//                    count2++;
//                }
                if ((releaseFrequency >= 50 || releaseFrequency2 >= 50) && (sum1 <=10 || sum2 <=10)) {
//                if (releaseFrequency >= 50 || releaseFrequency2 >= 50) {
                    csvPrinter.printRecord(record);
                    count3++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(String.format("%d\t%d\t%d", count1, count2, count3));
    }

    @Test
    public void filter2() {

        Set<String> uidSet = new HashSet<>();
        try (CSVParser csvParser1 = CsvFileHelper.reader("C:\\Users\\cl32\\Documents\\weibo\\weiboBigGraphNew\\uid_core_new/uid_blue_v.csv")) {
            for (CSVRecord record : csvParser1) {
                uidSet.add(record.get(0));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try (CSVParser csvParser = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/user_blog_stat_220w_inactivity.csv", HEADER_USER_BLOG_STAT, true);
             CSVPrinter csvPrinter = CsvFileHelper.writer("C:/Users/cl32/Desktop/stat/user_blog_stat_220w_inactivity_blue.csv", HEADER_USER_BLOG_STAT);
             CSVPrinter csvPrinter2 = CsvFileHelper.writer("C:/Users/cl32/Desktop/stat/user_blog_stat_220w_inactivity_personal.csv", HEADER_USER_BLOG_STAT)) {
            for (CSVRecord record : csvParser) {
                String uid = record.get(0);
                if (uidSet.contains(uid)){
                    csvPrinter.printRecord(record);
                } else {
                    csvPrinter2.printRecord(record);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void filter3() {

        try (CSVParser csvParser = CsvFileHelper.reader("C:\\Users\\cl32\\Desktop\\stat\\user_filter_new/user_blog_stat_220w_suspected_match_2.csv");
             CSVPrinter csvPrinter = CsvFileHelper.writer("C:\\Users\\cl32\\Desktop\\stat\\user_filter_new/user_220w_delete_new2.csv")) {
            for (CSVRecord record : csvParser) {
                String per = record.get(1);
                float v = Float.parseFloat(per);
                if (v >= 0.4){
                    csvPrinter.printRecord(record);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void filter4() {

        Set<String> uidSet = new HashSet<>();
//        try (CSVParser csvParser1 = CsvFileHelper.reader("C:\\Users\\cl32\\Documents\\weibo\\weiboBigGraphNew\\uid_delete/uid_core_delete2.csv")) {
//        try (CSVParser csvParser1 = CsvFileHelper.reader("C:\\Users\\cl32\\Desktop\\user_youhuiquan/all_clean.csv")) {
        try (CSVParser csvParser1 = CsvFileHelper.reader("C:\\Users\\cl32\\Documents\\weibo\\weiboBigGraphNew\\uid_delete/非优惠券号.csv")) {
            for (CSVRecord record : csvParser1) {
                uidSet.add(record.get(0));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//
//        try (CSVParser csvParser = CsvFileHelper.reader("C:/Users/cl32/Desktop/stat/user_blog_stat.csv");
//             CSVPrinter csvPrinter = CsvFileHelper.writer("C:\\Users\\cl32\\Desktop\\user_youhuiquan/user_blog_stat_car.csv", HEADER_USER_BLOG_STAT)) {
        try (CSVParser csvParser = CsvFileHelper.reader("C:\\Users\\cl32\\Desktop\\stat\\user_filter_new\\user_suspected_new/uid_suspected.csv");
             CSVPrinter csvPrinter = CsvFileHelper.writer("C:\\Users\\cl32\\Desktop\\stat\\user_filter_new\\user_suspected_new/uid_suspected_20190929.csv")) {
            for (CSVRecord record : csvParser) {
                String uid = record.get(0);
                if (!uidSet.contains(uid)){
                    csvPrinter.printRecord(record);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void filte5() throws IOException {

        Set<String> uidSet = new HashSet<>();
//        try (CSVParser csvParser1 = CsvFileHelper.reader("C:\\Users\\cl32\\Documents\\weibo\\weiboBigGraphNew\\uid_delete/uid_core_delete2.csv")) {
        try (CSVParser csvParser1 = CsvFileHelper.reader("C:\\Users\\cl32\\Desktop\\user_youhuiquan/youhuiquan.txt")) {
            for (CSVRecord record : csvParser1) {
                uidSet.add(record.get(0));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        Set<String> uidSet2 = new HashSet<>();
        try (CSVParser csvParser = CsvFileHelper.reader("C:\\Users\\cl32\\Desktop\\stat\\user_filter_new/user_filter_name3.csv", new String[]{},true);
             CSVParser csvParser2 = CsvFileHelper.reader("C:\\Users\\cl32\\Desktop\\stat\\user_filter_new/user_220w_delete_new2.csv")) {
            for (CSVRecord record : csvParser) {
                String uid = record.get(0);
                uidSet2.add(uid);
            }
            for (CSVRecord record : csvParser2) {
                String uid = record.get(0);
                uidSet2.add(uid);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(uidSet.size());
        System.out.println(uidSet2.size());
        uidSet.removeAll(uidSet2);
        write("C:\\Users\\cl32\\Desktop\\user_youhuiquan/yhx_2.csv", uidSet);
        System.out.println(uidSet.size());

    }

    private void write(String file, Set<String> set) throws IOException {
        try (CSVPrinter csvPrinter = CsvFileHelper.writer(file);
             CSVParser csvParser = CsvFileHelper.reader("C:\\Users\\cl32\\Desktop\\user_youhuiquan/yhx_name.txt")){
            for(CSVRecord record: csvParser) {
                if (set.contains(record.get(0))){
                    csvPrinter.printRecord(record);
                }
            }
        }
    }

    @Test
    public void filte6() throws IOException {

        String fileDirName = "C:\\Users\\cl32\\Desktop\\stat\\user_filter_new\\uid_delete";
        File fileDir = new File(fileDirName);
        File[] files = fileDir.listFiles();
        Set<String> uidSet = new HashSet<>();
        for (File file : files) {
            try (CSVParser csvParser1 = CsvFileHelper.reader(file)) {
                for (CSVRecord record : csvParser1) {
                    uidSet.add(record.get(0));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try (CSVPrinter csvPrinter = CsvFileHelper.writer("C:\\Users\\cl32\\Desktop\\stat\\user_filter_new/uid_core_delete.csv")){
            uidSet.forEach(s -> {
                try {
                    csvPrinter.printRecord(s);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

//
//        try (CSVParser csvParser1 = CsvFileHelper.reader("C:\\Users\\cl32\\Desktop\\user_youhuiquan/yhx_2.csv")) {
//            for (CSVRecord record : csvParser1) {
//                uidSet.add(record.get(0));
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        try (CSVParser csvParser = CsvFileHelper.reader("C:\\Users\\cl32\\Documents\\weibo/weibo.csv", new String[]{},true);
//             CSVPrinter csvPrinter = CsvFileHelper.writer("C:\\Users\\cl32\\Desktop\\stat\\user_filter_new/user_220w_delete_new3.csv")) {
//            for (CSVRecord record : csvParser) {
//                String uid = record.get(0);
//                if (uidSet.contains(uid)){
//                    csvPrinter.printRecord(record);
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

    }
}