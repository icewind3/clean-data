package com.cl.data.file;

import com.cl.data.file.service.UidMidBlogFileService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

@RunWith(SpringRunner.class)
@SpringBootTest
public class DataFileApplicationTests {

    @Resource
    private UidMidBlogFileService uidMidBlogFileService;

    @Test
    public void contextLoads() {
        String filePath = "C:\\Users\\cl32\\Desktop\\mblog_from_uid2";
        String resultPath = "C:\\Users\\cl32\\Desktop\\uid_mid_blog2";
        uidMidBlogFileService.cleanToUidMidBlogFile(filePath, resultPath);
    }


}
