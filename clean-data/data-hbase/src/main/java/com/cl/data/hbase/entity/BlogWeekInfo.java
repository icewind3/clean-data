package com.cl.data.hbase.entity;

import lombok.Getter;
import lombok.Setter;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author yejianyu
 * @date 2019/10/25
 */
@Getter
@Setter
public class BlogWeekInfo {
    long uid;
    long blogTotal = 0L;
    long retweetBlogTotal = 0L;

    long attitudeSum = 0L;
    long commentSum = 0L;
    long repostSum = 0L;

    long repostBlogAttitudeSum = 0L;
    long repostBlogCommentSum = 0L;
    long repostBlogRepostSum = 0L;
    String startDate;
    String endDate;

    public BlogWeekInfo(long uid) {
        this.uid = uid;
    }

    public void addAttitudeCount(int count, boolean isRetweeted) {
        if (isRetweeted) {
            repostBlogAttitudeSum += count;
        }
        attitudeSum += count;
    }

    public void addCommentCount(int count, boolean isRetweeted) {
        if (isRetweeted) {
            repostBlogCommentSum += count;
        }
        commentSum += count;
    }

    public void addRepostCount(int count, boolean isRetweeted) {
        if (isRetweeted) {
            repostBlogRepostSum += count;
        }
        repostSum += count;
    }

    public void increaseBlogCount(boolean isRetweeted) {
        if (isRetweeted) {
            retweetBlogTotal++;
        }
        blogTotal++;
    }

    public void setDateRange(Timestamp createTime) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(createTime.getTime());
        int dayWeek = calendar.get(Calendar.DAY_OF_WEEK);
        if (dayWeek == 1) {
            dayWeek = 7;
        } else {
            dayWeek -= 1;
        }
        // 计算本周开始的时间
        calendar.add(Calendar.DAY_OF_MONTH, 1 - dayWeek);
        Date startDate = calendar.getTime();
        calendar.add(Calendar.DAY_OF_MONTH, 6);
        Date endDate = calendar.getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        this.startDate = sdf.format(startDate);
        this.endDate = sdf.format(endDate);
    }

    @Override
    public String toString() {
        return uid + "," + startDate + "," + endDate + "," + blogTotal + "," + retweetBlogTotal + "," + attitudeSum
            + "," + commentSum + "," + repostSum + "," + repostBlogAttitudeSum + "," + repostBlogCommentSum + ","
            + repostBlogRepostSum;
    }
}