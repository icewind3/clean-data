package com.cl.graph.weibo.data.config;

import com.alibaba.druid.spring.boot.autoconfigure.DruidDataSourceBuilder;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;

/**
 * @author yejianyu
 * @date 2019/7/19
 */
@Configuration
public class WeiboMarketing2DataSourceConfiguration {

    @Bean(name = "weiboMarketing2DataSource")
    @ConfigurationProperties(prefix = "datasource.weibo-marketing2")
    public DataSource dataSource() {
        return DruidDataSourceBuilder.create().build();
    }

    @Bean(name = "weiboMarketing2DataSourceTransactionManager")
    public DataSourceTransactionManager dataSourceTransactionManager(@Qualifier("weiboMarketing2DataSource")DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean(name = "weiboMarketing2SqlSessionFactory")
    public SqlSessionFactory sessionFactory(@Qualifier("weiboMarketing2DataSource")DataSource dataSource) throws Exception {
        SqlSessionFactoryBean sessionFactoryBean = new SqlSessionFactoryBean();
        sessionFactoryBean.setMapperLocations(new PathMatchingResourcePatternResolver()
                .getResources("classpath:mybatis/mapper/marketing2/*.xml"));
        sessionFactoryBean.setConfigLocation(new PathMatchingResourcePatternResolver()
                .getResource("classpath:mybatis/mybatis-config.xml"));
        sessionFactoryBean.setDataSource(dataSource);
        return sessionFactoryBean.getObject();
    }

    @Bean("weiboMarketing2SqlSessionTemplate")
    public SqlSessionTemplate sqlSessionTemplate(@Qualifier("weiboMarketing2SqlSessionFactory") SqlSessionFactory sqlSessionFactory) {
        return new SqlSessionTemplate(sqlSessionFactory);
    }
}
