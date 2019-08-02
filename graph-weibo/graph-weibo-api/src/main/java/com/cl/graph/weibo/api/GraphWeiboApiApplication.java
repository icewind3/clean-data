package com.cl.graph.weibo.api;

import org.mybatis.spring.annotation.MapperScan;
import org.mybatis.spring.annotation.MapperScans;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@EnableTransactionManagement
@SpringBootApplication
@MapperScans(value = {
        @MapperScan(basePackages = "com.cl.graph.weibo.api.mapper.weibo",
                sqlSessionFactoryRef = "weiboSqlSessionFactory", sqlSessionTemplateRef = "weiboSqlSessionTemplate"),
        @MapperScan(basePackages = "com.cl.graph.weibo.api.mapper.marketing",
                sqlSessionFactoryRef = "weiboMarketingSqlSessionFactory", sqlSessionTemplateRef = "weiboMarketingSqlSessionTemplate")
})
public class GraphWeiboApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(GraphWeiboApiApplication.class, args);
    }

}
