/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.kafka.support;

import java.util.Map;
import java.util.Properties;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisException;

import com.lamfire.logger.Logger;
import com.lamfire.utils.Maps;
import com.lamfire.utils.PropertiesUtils;
import com.lamfire.utils.StringUtils;

/**
 * @author zxc Mar 30, 2017 4:01:02 PM
 */
@SuppressWarnings("deprecation")
public class JedisHelper {

    private static Logger       _      = Logger.getLogger(JedisHelper.class);

    private static int          expire = 60 * 60 * 24 * 3;
    private static JedisPool    pool;

    private static Properties   jedis_pro;
    private static final String host;
    private static final int    port;

    static {
        jedis_pro = PropertiesUtils.load("redis.properties", JedisHelper.class);
        host = jedis_pro.getProperty("redis.server.host");
        port = Integer.parseInt(jedis_pro.getProperty("redis.server.port"));

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(Integer.parseInt(jedis_pro.getProperty("redis.maxActive")));
        config.setMaxIdle(Integer.parseInt(jedis_pro.getProperty("redis.maxIdle")));
        config.setMinIdle(Integer.parseInt(jedis_pro.getProperty("redis.minIdle")));
        config.setMaxWaitMillis(Long.parseLong(jedis_pro.getProperty("redis.maxWait")));
        config.setMinEvictableIdleTimeMillis(Long.parseLong(jedis_pro.getProperty("redis.minEvictableIdleTimeMillis")));
        config.setNumTestsPerEvictionRun(Integer.parseInt(jedis_pro.getProperty("redis.numTestsPerEvictionRun")));
        config.setTimeBetweenEvictionRunsMillis(Long.parseLong(jedis_pro.getProperty("redis.timeBetweenEvictionRunsMillis")));
        config.setTestOnBorrow(false);
        config.setTestOnReturn(false);
        config.setTestWhileIdle(false);
        pool = new JedisPool(config, host, port);
    }

    public Long hincrBy(String key, String field, Long value) {
        if (StringUtils.isEmpty(key) || value == null) return null;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            Long count = jedis.hincrBy(key, field, value);
            jedis.expire(key, expire);
            return count;
        } catch (Exception e) {
            _.error("incrBy error!key=" + key, e);
            return null;
        } finally {
            returnBrokenResource(jedis);
        }
    }

    public Map<String, String> hgetincrBy(String key) {
        if (StringUtils.isEmpty(key)) return Maps.newHashMap();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            return jedis.hgetAll(key);
        } catch (Exception e) {
            _.error("hgetincrBy error!key=" + key, e);
        } finally {
            returnBrokenResource(jedis);
        }
        return Maps.newHashMap();
    }

    public Long hgetCount(String key, String field) {
        if (StringUtils.isEmpty(key)) return 0l;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            return jedis.hget(key, field) == null ? 0l : Long.valueOf(jedis.hget(key, field));
        } catch (Exception e) {
            _.error("hset error!key=" + key, e);
        } finally {
            returnBrokenResource(jedis);
        }
        return 0l;
    }

    public void hset(String key, String field, String value) {
        hset(key, field, value, expire);
    }

    public void hset(String key, String field, String value, int seconds) {
        if (StringUtils.isEmpty(key) || value == null) return;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.hset(key, field, value);
            jedis.expire(key, seconds);
        } catch (Exception e) {
            _.error("hset error!key=" + key, e);
        } finally {
            returnBrokenResource(jedis);
        }
    }

    public String hget(String key, String field) {
        if (StringUtils.isEmpty(key) || StringUtils.isBlank(field)) return null;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            return jedis.hget(key, field);
        } catch (Exception e) {
            _.error("hset error!key=" + key, e);
            return null;
        } finally {
            returnBrokenResource(jedis);
        }
    }

    public Map<String, String> hgetAll(String key) {
        if (StringUtils.isEmpty(key)) return null;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            return jedis.hgetAll(key);
        } catch (Exception e) {
            _.error("hset error!key=" + key, e);
            return null;
        } finally {
            returnBrokenResource(jedis);
        }
    }

    public long hlen(String key) {
        if (StringUtils.isEmpty(key)) return 0l;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            return jedis.hlen(key);
        } catch (Exception e) {
            _.error("hlen error!key=" + key, e);
            return 0;
        } finally {
            returnBrokenResource(jedis);
        }
    }

    public void hdel(String key, String field) {
        if (StringUtils.isEmpty(key)) return;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (field != null) {
                jedis.hdel(key, field);
            } else {
                jedis.hdel(key);
            }
        } catch (Exception e) {
            _.error("hdel error!key=" + key, e);
        } finally {
            returnBrokenResource(jedis);
        }
    }

    public void del(String key) {
        if (StringUtils.isEmpty(key)) return;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.del(key);
        } catch (Exception e) {
            _.error("del error!key=" + key, e);
        } finally {
            returnBrokenResource(jedis);
        }
    }

    public void expire(String key, int seconds) {
        if (StringUtils.isEmpty(key)) return;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.expire(key, seconds);
        } catch (Exception e) {
            _.error("expire error!key=" + key, e);
        } finally {
            returnBrokenResource(jedis);
        }
    }

    public void addHLL(String key, String... value) {
        if (StringUtils.isEmpty(key) || value == null) return;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.pfadd(key, value);
        } catch (Exception e) {
            _.error("add error!key=" + key, e);
        } finally {
            if (jedis != null) jedis.close();
        }
    }

    public long countHLL(String key) {
        if (StringUtils.isEmpty(key)) return 0l;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            return jedis.pfcount(key);
        } catch (Exception e) {
            _.error("count error!key=" + key, e);
            return 0;
        } finally {
            if (jedis != null) jedis.close();
        }
    }

    /**
     * 获取资源
     * 
     * @return
     * @throws JedisException
     */
    public static Jedis getResource() throws JedisException {
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
        } catch (JedisException e) {
            _.warn("getResource.", e);
            returnBrokenResource(jedis);
            throw e;
        }
        return jedis;
    }

    /**
     * 归还资源
     * 
     * @param jedis
     */
    public static void returnBrokenResource(Jedis jedis) {
        if (jedis != null) pool.returnBrokenResource(jedis);
    }

    /**
     * 释放资源
     * 
     * @param jedis
     */
    public static void returnResource(Jedis jedis) {
        if (jedis != null) pool.returnResource(jedis);
    }
}
