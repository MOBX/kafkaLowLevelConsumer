/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.kafka.offset;

import java.io.Closeable;
import java.io.IOException;

import com.mob.kafka.support.JedisHelper;

/**
 * 基于redis实现的kafka offset提交
 * 
 * @author zxc Mar 30, 2017 3:56:05 PM
 */
public class RedisOffsetController implements OffsetController, Closeable {

    private static JedisHelper jedis;
    private String             key;

    public RedisOffsetController(String group, String topic, int partition) throws OffsetException {
        if (jedis == null) jedis = new JedisHelper();
        key = group + "-" + topic + "-" + partition;
    }

    @Override
    public boolean commitOffset(long offset) {
        try {
            jedis.hset("RedisOffset", key, String.valueOf(offset));
            return true;
        } catch (OffsetException e) {
            logger.error("RedisOffsetController commitOffset OffsetException", e);
        }
        return false;
    }

    @Override
    public long getLastOffset() {
        try {
            String value = jedis.hget("RedisOffset", key);
            if (value == null) return -1;
            return Long.parseLong(new String(value));
        } catch (OffsetException e) {
            logger.error("RedisOffsetController getLastOffset OffsetException ", e);
        }
        return -1;
    }

    @Override
    public void close() throws IOException {

    }
}
