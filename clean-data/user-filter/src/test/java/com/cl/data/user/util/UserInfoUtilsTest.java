package com.cl.data.user.util;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author yejianyu
 * @date 2019/9/24
 */
public class UserInfoUtilsTest {

    @Test
    public void isNameMatching() {
    }

    @Test
    public void isDescMatching() {
        boolean descMatching = UserInfoUtils.isDescMatching("一本分享世界顶级豪宅，探索当代艺术及新兴艺术家的杂志。已发行纸质版及双语电子版：英/汉语。contact@luxurypublications.com 44 (0) 203 445 0679");
        System.out.println(descMatching);
    }
}