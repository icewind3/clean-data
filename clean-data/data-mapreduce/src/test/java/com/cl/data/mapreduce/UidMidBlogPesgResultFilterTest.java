package com.cl.data.mapreduce;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/9/11
 */
public class UidMidBlogPesgResultFilterTest {

    @Test
    public void main() throws IOException {
        String a = "3959452987,4237012242576183,{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},\"{'连裤袜': 1, '丝袜': 1}\",{},{},{},{},{},{},{},{},{},{},{},{},{},2018-05-07 00:00:00,\n";
        CSVParser parse = CSVParser.parse(a, CSVFormat.DEFAULT);
        parse.forEach(record -> {
            System.out.println(record.get(0));
        });
    }

    @Test
    public void compare() throws IOException {
        String a = "2";
        String b = "10000001";
        int i = a.compareTo(b);
        System.out.println(i);
    }

    @Test
    public void testTreeSet() throws IOException {
        NavigableSet<MidCount> set =  new TreeSet<>(Comparator.reverseOrder());

        set.add(new MidCount("11", 3));
        set.add(new MidCount("12", 7));
        set.add(new MidCount("13", 2));
        MidCount midCount = set.last();
        System.out.println(midCount.getMid());
        for (MidCount a : set) {
            System.out.println(a.getMid());
        }
    }

    @Test
    public void a(){
        Timestamp timestamp = Timestamp.valueOf("2018-12-31 00:00:00");
        Calendar calendar = Calendar.getInstance();
        calendar.setFirstDayOfWeek(Calendar.MONDAY);//设置星期一为一周开始的第一天
        calendar.setMinimalDaysInFirstWeek(4);//可以不用设置
        calendar.setTimeInMillis(timestamp.getTime());//获得当前的时间戳
        int weekYear = calendar.get(Calendar.YEAR);//获得当前的年
        int weekOfYear = calendar.get(Calendar.WEEK_OF_YEAR);//获得当前日期属于今年的第几周

        System.out.println("第几周："+weekOfYear);
        calendar.setWeekDate(weekYear, weekOfYear, 2);//获得指定年的第几周的开始日期
        long starttime = calendar.getTime().getTime();//创建日期的时间该周的第一天，
        calendar.setWeekDate(weekYear, weekOfYear, 1);//获得指定年的第几周的结束日期
        long endtime = calendar.getTime().getTime();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String dateStart = simpleDateFormat.format(starttime);//将时间戳格式化为指定格式
        String dateEnd = simpleDateFormat.format(endtime);
        System.out.println(dateStart);
        System.out.println(dateEnd);
    }


    @Test
    public void b(){
        Timestamp timestamp = Timestamp.valueOf("2019-9-30 00:00:00");
        Calendar calendar = Calendar.getInstance();
//        calendar.setFirstDayOfWeek(Calendar.MONDAY);//设置星期一为一周开始的第一天
//        calendar.setMinimalDaysInFirstWeek(4);//可以不用设置
        calendar.setTimeInMillis(timestamp.getTime());//获得当前的时间戳
        int dayWeek = calendar.get(Calendar.DAY_OF_WEEK);
        if (dayWeek == 1) {
            dayWeek = 7;
        } else {
            dayWeek -= 1;
        }
        System.out.println("前时间是本周的第几天:" + dayWeek); // 输出要当前时间是本周的第几天
        // 计算本周开始的时间
        calendar.add(Calendar.DAY_OF_MONTH, 1 - dayWeek);
        Date startDate = calendar.getTime();
        calendar.add(Calendar.DAY_OF_MONTH, 6);
        Date endDate = calendar.getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        System.out.println("本周开始时间（周一）：" + sdf.format(startDate) + "==当前时间：" + sdf.format(endDate));
    }

    class MidCount implements Comparable<MidCount> {
        String mid;
        long count;

        MidCount(String mid, long count) {
            this.mid = mid;
            this.count = count;
        }

        @Override
        public int compareTo(MidCount o) {
            if (this.count > o.count) {
                return 1;
            }
            if (this.count < o.count) {
                return -1;
            }
            return Long.compare(Long.parseLong(this.mid), Long.parseLong(o.mid));
        }

        public String getMid() {
            return mid;
        }

        public void setMid(String mid) {
            this.mid = mid;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }
    }
}