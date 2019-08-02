package com.cl.graph.weibo.data.config;

import com.alibaba.druid.spring.boot.autoconfigure.DruidDataSourceBuilder;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;

/**
 * @author yejianyu
 * @date 2019/7/19
 */
@Configuration
public class MblogDataSourceConfiguration {

    @Bean(name = "mblogDataSource")
    @ConfigurationProperties(prefix = "datasource.mblog")
    public DataSource dataSource() {
        return DruidDataSourceBuilder.create().build();
    }

    @Bean(name = "mblogDataSourceTransactionManager")
    public DataSourceTransactionManager dataSourceTransactionManager(@Qualifier("mblogDataSource")DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean(name = "mblogSqlSessionFactory")
    public SqlSessionFactory sessionFactory(@Qualifier("mblogDataSource")DataSource dataSource) throws Exception {
        SqlSessionFactoryBean sessionFactoryBean = new SqlSessionFactoryBean();
        sessionFactoryBean.setMapperLocations(new PathMatchingResourcePatternResolver()
                .getResources("classpath:mybatis/mapper/mblog/*.xml"));
        sessionFactoryBean.setConfigLocation(new PathMatchingResourcePatternResolver()
                .getResource("classpath:mybatis/mybatis-config.xml"));
        sessionFactoryBean.setDataSource(dataSource);
        return sessionFactoryBean.getObject();
    }

    @Bean("mblogSqlSessionTemplate")
    public SqlSessionTemplate sqlSessionTemplate(@Qualifier("mblogSqlSessionFactory") SqlSessionFactory sqlSessionFactory) {
        return new SqlSessionTemplate(sqlSessionFactory);
    }
}
