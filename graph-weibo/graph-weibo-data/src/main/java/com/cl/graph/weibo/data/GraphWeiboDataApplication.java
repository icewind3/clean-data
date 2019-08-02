package com.cl.graph.weibo.data;

import org.mybatis.spring.annotation.MapperScan;
import org.mybatis.spring.annotation.MapperScans;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@EnableTransactionManagement
@SpringBootApplication
@MapperScans(value = {
        @MapperScan(basePackages = "com.cl.graph.weibo.data.mapper.weibo",
                sqlSessionFactoryRef = "weiboSqlSessionFactory", sqlSessionTemplateRef = "weiboSqlSessionTemplate"),
        @MapperScan(basePackages = "com.cl.graph.weibo.data.mapper.marketing",
                sqlSessionFactoryRef = "weiboMarketingSqlSessionFactory", sqlSessionTemplateRef = "weiboMarketingSqlSessionTemplate"),
        @MapperScan(basePackages = "com.cl.graph.weibo.data.mapper.marketing2",
                sqlSessionFactoryRef = "weiboMarketing2SqlSessionFactory", sqlSessionTemplateRef = "weiboMarketing2SqlSessionTemplate"),
        @MapperScan(basePackages = "com.cl.graph.weibo.data.mapper.mblog",
                sqlSessionFactoryRef = "mblogSqlSessionFactory", sqlSessionTemplateRef = "mblogSqlSessionTemplate")
})
public class GraphWeiboDataApplication {

    public static void main(String[] args) {
        SpringApplication.run(GraphWeiboDataApplication.class, args);
    }

}
