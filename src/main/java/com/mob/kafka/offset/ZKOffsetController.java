/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.kafka.offset;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

import com.mob.kafka.support.ZKUtils;

/**
 * 基于zookeeper实现的kafka offset提交
 * 
 * @author zxc Mar 30, 2017 3:59:36 PM
 */
public class ZKOffsetController implements OffsetController, Closeable {

    private ZKUtils zkUtils;
    private String  group;
    private String  topic;
    private int     partition;

    public ZKOffsetController(Properties prop, String group, String topic, int partition) {
        String zookeeperConnection = prop.getProperty(ZKUtils.ZOOKEEPER_CONNECT);
        int sessionTimeout = Integer.parseInt(prop.getProperty(ZKUtils.ZOOKEEPER_SESSION_TIMEOUT_MS, "10000"));
        int connectionTimeout = Integer.parseInt(prop.getProperty(ZKUtils.ZOOKEEPER_CONNECTION_TIMEOUT_MS, "10000"));
        String rootPath = prop.getProperty(ZKUtils.KAFKA_CUSTOM_CONSUMER_PATH, "/tmp/kafka/consumer");
        zkUtils = new ZKUtils(zookeeperConnection, sessionTimeout, connectionTimeout, rootPath);
        this.group = group;
        this.topic = topic;
        this.partition = partition;
    }

    @Override
    public boolean commitOffset(long offset) {
        zkUtils.setLastCommit(group, topic, partition, offset, false);
        return true;
    }

    @Override
    public long getLastOffset() {
        return zkUtils.getLastCommit(group, topic, partition);
    }

    @Override
    public void close() throws IOException {
        zkUtils.close();
    }
}
