package com.cl.graph.weibo.data.util;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.yaml.snakeyaml.Yaml;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Collections;

/**
 * @author zcc
 * @date 2019/6/26 14:43
 */
public class RedisLock {

    private PropertyPlaceholderUtil strictHelper;

    /** Prefix for system property placeholders: "${". */
    private static final String PLACEHOLDER_PREFIX = "${";
    /** Suffix for system property placeholders: "}". */
    private static final String PLACEHOLDER_SUFFIX = "}";
    /** Value separator for system property placeholders: ":". */
    private static final String VALUE_SEPARATOR = ":";

    private static final String LOCK_SUCCESS = "OK";
    private static final String SET_IF_NOT_EXIST = "NX";
    private static final String SET_WITH_EXPIRE_TIME = "PX";

    public static final RedisLock REDIS_LOCK = new RedisLock();

    private static final Long RELEASE_SUCCESS = 1L;

    private final JedisPool jedisPool;

    private RedisEntity entity;

    private RedisLock() {
    }

    {
        Yaml yaml = new Yaml();
        InputStream resourceAsStream = RedisLock.class.getClassLoader().getResourceAsStream("application.yml");
        if (resourceAsStream != null) {
            entity = null;
            try {
                Iterable<Object> objects = yaml.loadAll(resourceAsStream);
                Object obj = objects.iterator().next();
                JSONObject jsonObject = JSONObject.parseObject(JSONObject.toJSONString(obj));
                entity = jsonObject.getObject("redis", RedisEntity.class);
                resolveObject(entity);
                resourceAsStream.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

            JedisPoolConfig config = new JedisPoolConfig();
            //控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；
            //如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
            config.setMaxTotal(entity.getPool().getMaxActive());
            //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
            config.setMaxIdle(entity.getPool().getMaxIdle());
            //表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
            config.setMaxWaitMillis(entity.getPool().getMaxWait());
            //在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
            config.setTestOnBorrow(entity.getPool().getTestOnBorrow());

            jedisPool = new JedisPool(config, entity.getHost(), Integer.parseInt(entity.getPort()), entity.getPool().getTimeout(), entity.getPassword());
        } else {
            jedisPool = null;
        }
    }

    private void resolveObject(Object object) throws IllegalAccessException {
        Field[] fields = object.getClass().getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            Object o = field.get(object);
            if (isPrimitive(o)) {
                continue;
            }
            if ((o instanceof String) && ((String) o).startsWith(PLACEHOLDER_PREFIX) && ((String) o).endsWith(PLACEHOLDER_SUFFIX)) {
                String result = resolveRequiredPlaceholders(o.toString());
                field.set(object, result);
                continue;
            }
            resolveObject(o);
        }
    }

    private boolean isPrimitive(Object obj) {
        try {
            Class<?> aClass = obj.getClass();
            return ((Class<?>)aClass.getField("TYPE").get(null)).isPrimitive();
        } catch (Exception e) {
            return false;
        }
    }

    private PropertyPlaceholderUtil createPlaceholderUtil(boolean ignoreUnresolvablePlaceholders) {
        return new PropertyPlaceholderUtil(PLACEHOLDER_PREFIX, PLACEHOLDER_SUFFIX,
                VALUE_SEPARATOR, ignoreUnresolvablePlaceholders);
    }

    /**
     * 将以${开头的字符以spring的方式进行替换
     * @param text
     * @return
     * @throws IllegalArgumentException
     */
    public String resolveRequiredPlaceholders(String text) throws IllegalArgumentException {
        if (this.strictHelper == null) {
            this.strictHelper = createPlaceholderUtil(false);
        }
        return doResolvePlaceholders(text, this.strictHelper);
    }

    private String doResolvePlaceholders(String text, PropertyPlaceholderUtil helper) {
        return helper.replacePlaceholders(text);
    }

    /**
     * 从jedis连接池中获取获取jedis对象
     *
     * @return
     */
    public Jedis getJedis() {
        return jedisPool.getResource();
    }

    /**
     * 回收jedis(放到finally中)
     *
     * @param jedis
     */
    private void returnJedis(Jedis jedis) {
        if (null != jedis && null != jedisPool) {
            jedisPool.returnResource(jedis);
        }
    }

    public void closeJedisPool() {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    @Data
    private static class RedisEntity {
        private Pool pool;
        private String host;
        private String port;
        private String password;
        /**
         * 锁超时时间 ms
         */
        private Integer expireTime;
        /* 锁等待时间 ms */
        private Integer lockTime;
        /* 默认的睡眠时间  ms */
        private Integer sleepTime;
    }

    @Data
    private static class Pool {
        private Integer maxActive;
        private Integer maxIdle;
        private Integer maxWait;
        private Boolean testOnBorrow;
        private Integer timeout;
    }

    private boolean tryGetDistributedLock(Jedis jedis, String lockKey, String requestId, int expireTime) {
        String result = jedis.set(lockKey, requestId, SET_IF_NOT_EXIST, SET_WITH_EXPIRE_TIME, expireTime);
        return LOCK_SUCCESS.equals(result);
    }

    /**
     * 用默认的有效时间加锁
     */
    public boolean lock(Jedis jedis, String lockKey, String requestId) throws InterruptedException {
        return lock(jedis, lockKey, requestId, entity.getExpireTime());
    }

    /**
     * 加锁方法
     *
     * @param jedis      jedis连接对象
     * @param lockKey    锁的标识
     * @param requestId  当前请求的标识  （UUID）
     * @param expireTime 锁有效时间
     * @return
     * @throws InterruptedException
     */
    public boolean lock(Jedis jedis, String lockKey, String requestId, int expireTime) throws InterruptedException {
        long nowTime = System.currentTimeMillis();

        while (!tryGetDistributedLock(jedis, lockKey, requestId, expireTime)) {
            if (System.currentTimeMillis() - nowTime > entity.getLockTime()) {
                return false;
            }
            Thread.sleep(entity.getSleepTime());
        }
        return true;
    }

    /**
     * 释放分布式锁 ,并回收jedis(放到finally中)
     *
     * @param lockKey   锁
     * @param requestId 请求标识
     * @return 是否释放成功
     */
    public boolean releaseDistributedLock(Jedis jedis, String lockKey, String requestId) {
        String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
        Object result = jedis.eval(script, Collections.singletonList(lockKey), Collections.singletonList(requestId));
        returnJedis(jedis);
        return RELEASE_SUCCESS.equals(result);
    }
}
