package com.cl.graph.weibo.data.service;

import com.alibaba.fastjson.JSON;
import com.cl.graph.weibo.core.linux.LinuxAction;
import com.cl.graph.weibo.core.linux.LinuxUtil;
import com.cl.graph.weibo.core.linux.ResultApi;
import com.cl.graph.weibo.data.BaseSpringBootTest;
import com.cl.graph.weibo.data.dto.FollowingRelationshipDTO;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

/**
 * @author yejianyu
 * @date 2019/7/18
 */
public class FollowingRelationshipServiceTest extends BaseSpringBootTest {

    @Autowired
    private FollowingRelationshipService followingRelationshipService;

    @Test
    public void genFollowingRelationshipFile() {
        String file = "C:/Users/cl32/Downloads/微博大图/attention/20181229";
        String resultFile = "C:/Users/cl32/Downloads/微博大图";
//       followingRelationshipService.genFollowingRelationshipFile(file, resultFile);
       followingRelationshipService.genFollowingRelationshipFileFromRemote("","");

    }

    @Test
    public void genFollowingRelationshipFileRemote() {
        String file = "C:/Users/cl32/Downloads/微博大图/attention/20181229";
        String resultFile = "C:/Users/cl32/Downloads/微博大图";
        LinuxAction linuxAction = LinuxUtil.getSingletonLinuxAction("192.168.2.16", "cldev", "cldev");
        ResultApi<List<String>> resultApi = linuxAction.executeSuccess("ls /data8/mysql/load_data/relationship/115.159.22.200");
        System.out.println(resultApi.getData());
    }

    @Test
    public void test2() {
        String filePath = "C:/Users/cl32/Downloads/weioboBigGraph/result/20190722";
        String remoteDir = "/data6/weiboBigGraph2/result";
        LinuxAction linuxAction = LinuxUtil.getSingletonLinuxAction("192.168.2.64", "xuwenlong", "xuwenlong2019");
        File file = new File(filePath);
        Deque<File> queue = new LinkedList<>();
//        queue.push();
        if (file.isDirectory()){
            File[] files = file.listFiles();


        }
        ResultApi<List<String>> resultApi = linuxAction.executeSuccess("mkdir -p " + remoteDir);

//        ResultApi resultApi = linuxAction.uploadFile(file, remoteDir, "0600");
        System.out.println(resultApi.getData());
    }


    @Test
    public void test3(){
        followingRelationshipService.genFollowingRelationshipFile2("" ,"");
    }
}