package com.cl.data.hbase.dto;

import lombok.Data;

import java.sql.Timestamp;
import java.util.List;

/**
 * @author yejianyu
 * @date 2019/8/9
 */
@Data
public class UserBlogListDTO {

    private Long uid;
    private List<BlogDTO> attitudeBlogList;
    private List<BlogDTO> commentBlogList;
    private List<BlogDTO> repostBlogList;
    private List<BlogDTO> blogList;

    public UserBlogListDTO(Long uid) {
        this.uid = uid;
    }
}
