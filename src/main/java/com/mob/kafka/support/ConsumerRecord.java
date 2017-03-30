/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.kafka.support;

/**
 * @author zxc Mar 30, 2017 3:59:36 PM
 */
public class ConsumerRecord {

    private long   offset;
    private byte[] key;
    private byte[] value;

    public ConsumerRecord(long offset, byte[] key, byte[] value) {
        this.offset = offset;
        this.key = key;
        this.value = value;
    }

    public long getOffset() {
        return offset;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public String toString() {
        return "offset=" + offset + "; key=" + (key != null ? new String(key) : "") + "; value=" + new String(value);
    }
}
