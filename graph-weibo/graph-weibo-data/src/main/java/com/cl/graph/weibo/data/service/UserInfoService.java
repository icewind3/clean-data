package com.cl.graph.weibo.data.service;

import com.alibaba.fastjson.JSON;
import com.cl.graph.weibo.core.exception.ServiceException;
import com.cl.graph.weibo.data.constant.SSDBConstants;
import com.cl.graph.weibo.data.entity.BigVCategory;
import com.cl.graph.weibo.data.entity.FriendlyLinkTradeCompany;
import com.cl.graph.weibo.data.entity.UserInfo;
import com.cl.graph.weibo.data.mapper.weibo.BigVCategoryMapper;
import com.cl.graph.weibo.data.mapper.weibo.FriendlyLinkTradeCompanyMapper;
import com.cl.graph.weibo.data.mapper.weibo.UserInfoMapper;
import com.cl.graph.weibo.data.util.CsvFileHelper;
import com.cl.graph.weibo.data.util.UserInfoUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.nutz.ssdb4j.SSDBs;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author yejianyu
 * @date 2019/7/24
 */
@Slf4j
@Service
public class UserInfoService {

    @Value("${spring.redis.user-info.host}")
    private String ssdbHost;

    @Value("${spring.redis.user-info.port}")
    private Integer ssdbPort;

    @Resource
    private BigVCategoryMapper bigVCategoryMapper;

    @Resource
    private FriendlyLinkTradeCompanyMapper friendlyLinkTradeCompanyMapper;
    @Resource
    private UserInfoMapper userInfoMapper;
    private static final String[] HEADER_USER = {"ID", "昵称", "粉丝数", "关注数", "博文数", "领域标签1", "领域标签2", "领域标签3",
            "PGRank1", "PGRank2", "PGRank3", "PGRank4"};

    private static final String[] FRIENDLY_LINK_TRADE_COMPANY_SUFFIX = {"20190619", "20190618", "20190617"};

    public void genUserInfoFile(String resultPath, String tableSuffix) {

        int pageSize = 100000;
        long maxCount;
        long startTime = System.currentTimeMillis();
        log.info("开始处理userinfo_{}", tableSuffix);
        try {
            maxCount = userInfoMapper.count(tableSuffix);
        } catch (InvalidDataAccessResourceUsageException e) {
            String message = e.getMessage();
            if (StringUtils.containsIgnoreCase(message, "Table")
                    && StringUtils.containsIgnoreCase(message, "doesn't exist")) {
                log.warn("表userinfo_{}不存在", tableSuffix);
            } else {
                log.error("处理表userinfo_" + tableSuffix + "出错", e);
            }
            return;
        }
        log.info("统计hot_comment_{}的总数为{}, 耗时{}ms", tableSuffix, maxCount,
                System.currentTimeMillis() - startTime);
        int count = 0;
        int pageNum = 1;
        try (CSVPrinter blueWriter = CsvFileHelper.writer(resultPath + File.separator + "blue_v.csv", HEADER_USER);
             CSVPrinter yellowWriter = CsvFileHelper.writer(resultPath + File.separator + "yellow_v.csv", HEADER_USER);
             CSVPrinter redWriter = CsvFileHelper.writer(resultPath + File.separator + "red_v.csv", HEADER_USER);
             CSVPrinter showWriter = CsvFileHelper.writer(resultPath + File.separator + "show_v.csv", HEADER_USER);
             CSVPrinter personalWriter = CsvFileHelper.writer(resultPath + File.separator + "personal.csv", HEADER_USER);
             CSVPrinter fansWriter = CsvFileHelper.writer(resultPath + File.separator + "fans.csv", HEADER_USER)) {
            List<UserInfo> userInfoList = userInfoMapper.findAll(tableSuffix, pageNum, pageSize);
            count += userInfoList.size();
            for (UserInfo userInfo : userInfoList) {
                String tag = getDomainTag(userInfo);
                if (userInfo.isBlueV()) {
                    write(blueWriter, userInfo, tag);
                } else if (userInfo.isShowV()) {
                    write(showWriter, userInfo, tag);
                } else if (userInfo.isYellowV()) {
                    write(yellowWriter, userInfo, tag);
                } else if (userInfo.isRedV()) {
                    write(redWriter, userInfo, tag);
                } else if (userInfo.isCoreUser()) {
                    write(personalWriter, userInfo, tag);
                } else {
                    write(fansWriter, userInfo, tag);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("userinfo_{}已处理完成，实际生成{}条点数据, 共耗时{}ms", tableSuffix, count,
                System.currentTimeMillis() - startTime);
    }

    public void genUserFileByTraverse(String resultPath) throws ServiceException {
        long startTime = System.currentTimeMillis();
        log.info("开始遍历userInfo生成节点");
        try (SSDB ssdb = SSDBs.pool(ssdbHost, ssdbPort, 1000 * 100, null);
             CSVPrinter blueWriter = CsvFileHelper.writer(resultPath + File.separator + "blue_v.csv", HEADER_USER);
             CSVPrinter yellowWriter = CsvFileHelper.writer(resultPath + File.separator + "yellow_v.csv", HEADER_USER);
             CSVPrinter redWriter = CsvFileHelper.writer(resultPath + File.separator + "red_v.csv", HEADER_USER);
             CSVPrinter showWriter = CsvFileHelper.writer(resultPath + File.separator + "show_v.csv", HEADER_USER);
             CSVPrinter personalWriter = CsvFileHelper.writer(resultPath + File.separator + "personal.csv", HEADER_USER);
             CSVPrinter fansWriter = CsvFileHelper.writer(resultPath + File.separator + "fans.csv", HEADER_USER)) {
            String start = StringUtils.EMPTY;
            String end = StringUtils.EMPTY;
            final long[] max = {0};
            long index = 0;
            int stepSize = 1000;
            while (true) {
                if (max[0] != 0) {
                    start = String.valueOf(max[0]);
                }
                Response response = ssdb.hscan(SSDBConstants.SSDB_USER_INFO_KEY, start, end, stepSize);
                Map<String, String> map = response.mapString();
                map.forEach((k, v) -> {
                    UserInfo userInfo = JSON.parseObject(v, UserInfo.class);
                    max[0] = Long.parseLong(userInfo.getUid());
                    String tag = getDomainTag(userInfo);
                    try {
                        if (userInfo.isBlueV()) {
                            write(blueWriter, userInfo, tag);
                        } else if (userInfo.isShowV()) {
                            write(showWriter, userInfo, tag);
                        } else if (userInfo.isYellowV()) {
                            write(yellowWriter, userInfo, tag);
                        } else if (userInfo.isRedV()) {
                            write(redWriter, userInfo, tag);
                        } else if (userInfo.isCoreUser()) {
                            write(personalWriter, userInfo, tag);
                        } else {
                            write(fansWriter, userInfo, tag);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
                index++;
                if (index % 10 == 0) {
                    log.info("已处理index={}, step={}, 已耗时{}ms", index, stepSize, System.currentTimeMillis() - startTime);
                }
                if (map.size() < stepSize) {
                    break;
                }
                map.clear();
            }
        } catch (IOException e) {
            log.error("遍历userInfo出错", e);
            throw new ServiceException("遍历userInfo出错", e);
        }
        log.info("已完成节点文件生成，共耗时{}s", (System.currentTimeMillis() - startTime) / 1000);
    }

    private void write(CSVPrinter writer, UserInfo userInfo, String tag) throws IOException {
        writer.printRecord(userInfo.getUid(), userInfo.getName(), userInfo.getFollowersCount(),
                userInfo.getFriendsCount(), userInfo.getStatusesCount(), tag, StringUtils.EMPTY, StringUtils.EMPTY,
                StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY);
    }

    private String getDomainTag(UserInfo userInfo) {
        String uid = userInfo.getUid();
        if (userInfo.isPersonalBigV()) {
            List<BigVCategory> categoryByUid = bigVCategoryMapper.findCategoryByUid(uid);
            if (categoryByUid != null && !categoryByUid.isEmpty()) {
                return categoryByUid.get(0).getMinNavigation();
            }
            List<BigVCategory> peopleByUid = bigVCategoryMapper.findPeopleByUid(uid);
            if (peopleByUid != null && !peopleByUid.isEmpty()) {
                return peopleByUid.get(0).getMinNavigation();
            }
            return StringUtils.EMPTY;
        }
        if (userInfo.isBlueV()) {
            for (String suffix : FRIENDLY_LINK_TRADE_COMPANY_SUFFIX) {
                FriendlyLinkTradeCompany blueV = friendlyLinkTradeCompanyMapper.findByUid(uid, suffix);
                if (blueV != null) {
                    return blueV.getTrade();
                }
            }
        }
        return StringUtils.EMPTY;
    }
}
