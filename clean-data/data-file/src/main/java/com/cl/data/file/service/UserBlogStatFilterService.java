package com.cl.data.file.service;

import com.cl.graph.weibo.core.util.CsvFileHelper;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author yejianyu
 * @date 2019/9/12
 */
@Service
public class UserBlogStatFilterService {

    private static final String[] HEADER_USER_BLOG_STAT = {"uid", "releaseFrequency", "attitudeAvg", "commentAvg", "repostAvg",
        "attitudeMedian", "commentMedian", "repostMedian", "repostRate", "releaseFrequency2", "attitudeAvg2",
        "commentAvg2", "repostAvg2", "attitudeMedian2", "commentMedian2", "repostMedian2", "repostRate2", "attitudeTopAvg",
        "commentTopAvg", "repostTopAvg"};



}
