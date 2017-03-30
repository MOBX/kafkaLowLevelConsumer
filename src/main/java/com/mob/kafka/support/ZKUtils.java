/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.kafka.support;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import com.alibaba.fastjson.JSON;
import com.lamfire.logger.Logger;

/**
 * @author zxc Mar 30, 2017 3:59:36 PM
 */
public class ZKUtils implements Closeable {

    private static Logger      LOG                             = Logger.getLogger(ZKUtils.class);

    public static final String CONSUMERS_PATH                  = "/consumers";
    public static final String BROKER_IDS_PATH                 = "/brokers/ids";
    public static final String BROKER_TOPICS_PATH              = "/brokers/topics";
    public static final String ZOOKEEPER_CONNECT               = "zookeeper.connect";
    public static final String ZOOKEEPER_SESSION_TIMEOUT_MS    = "zookeeper.session.timeout.ms";
    public static final String ZOOKEEPER_CONNECTION_TIMEOUT_MS = "zookeeper.connection.timeout.ms";
    public static final String KAFKA_CUSTOM_CONSUMER_PATH      = "kafka.custom.consumer.path";

    private ZkClient           client;
    private String             rootPath;

    public ZKUtils(String zkConnection, int sessionTimeout, int connctionTimeout, String rootPath) {
        this.rootPath = rootPath;
        connect(zkConnection, sessionTimeout, connctionTimeout, rootPath);
    }

    private void connect(String zkConnection, int sessionTimeout, int connctionTimeout, String rootPath) {
        client = new ZkClient(zkConnection, sessionTimeout, connctionTimeout, new StringSerializer());
    }

    public String getBroker(String id) throws IOException {
        KafkaBroker kafkaBroker = JSON.parseObject((String) client.readData(BROKER_IDS_PATH + "/" + id), KafkaBroker.class);
        return kafkaBroker.getHost() + ":" + kafkaBroker.getPort();
    }

    public List<KafkaBroker> getBrokerList() {
        List<String> brokerIds = getChildrenParentMayNotExist(BROKER_IDS_PATH);
        List<KafkaBroker> brokerList = new ArrayList<KafkaBroker>();
        KafkaBroker kafkaBroker = null;
        for (String bid : brokerIds) {
            String data = client.readData(BROKER_IDS_PATH + "/" + bid);
            LOG.info("Broker " + bid + " " + data);
            kafkaBroker = JSON.parseObject(data, KafkaBroker.class);
            brokerList.add(kafkaBroker);
        }
        return brokerList;
    }

    public List<String> getPartitions(String topic) {
        List<String> partitions = new ArrayList<String>();
        KafkaPartitions kafkaPartitions = JSON.parseObject((String) client.readData(BROKER_TOPICS_PATH + "/" + topic), KafkaPartitions.class);
        for (String partitionId : kafkaPartitions.getPartitions().keySet()) {
            partitions.add(kafkaPartitions.getPartitions().get(partitionId)[0] + "-" + partitionId);
        }
        return partitions;
    }

    private String getOffsetsPath(String group, String topic, int partition) {
        return rootPath + "/" + group + "/offsets/" + topic + "/" + partition;
    }

    private String getTempOffsetsPath(String group, String topic, int partition) {
        return rootPath + "/" + group + "/offsets-temp/" + topic + "/" + partition;
    }

    private String getTempOffsetsPath(String group, String topic) {
        return rootPath + "/" + group + "/offsets-temp/" + topic;
    }

    public long getLastCommit(String group, String topic, int partition) {
        String znode = getOffsetsPath(group, topic, partition);
        String offset = client.readData(znode, true);

        if (offset == null) return -1L;
        return Long.valueOf(offset);
    }

    public void setLastCommit(String group, String topic, int partition, long commit, boolean temp) {
        String path = temp ? getTempOffsetsPath(group, topic, partition) : getOffsetsPath(group, topic, partition);
        if (!client.exists(path)) {
            client.createPersistent(path, true);
        }
        client.writeData(path, commit);
    }

    /**
     * commit temp offset
     * 
     * @param group
     * @param topic
     * @return
     */
    public boolean commit(String group, String topic) {
        List<String> partitions = getChildrenParentMayNotExist(getTempOffsetsPath(group, topic));
        for (String partition : partitions) {
            String path = getTempOffsetsPath(group, topic, Integer.parseInt(partition));
            String offset = client.readData(path);
            setLastCommit(group, topic, Integer.parseInt(partition), Long.valueOf(offset), false);
            client.delete(path);
        }
        return true;
    }

    private List<String> getChildrenParentMayNotExist(String path) {
        try {
            List<String> children = client.getChildren(path);
            return children;
        } catch (ZkNoNodeException e) {
            return new ArrayList<String>();
        }
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    static class StringSerializer implements ZkSerializer {

        public StringSerializer() {
        }

        @Override
        public Object deserialize(byte[] data) throws ZkMarshallingError {
            if (data == null) return null;
            return new String(data);
        }

        @Override
        public byte[] serialize(Object data) throws ZkMarshallingError {
            return data.toString().getBytes();
        }

    }

    public static void main(String[] args) throws IOException {
        Properties prop = new Properties();
        prop.setProperty(ZOOKEEPER_CONNECT, "192.168.180.73:2181");
        ZKUtils zkUtils = new ZKUtils("192.168.180.73:2181", 10000, 10000, "/tmp/kafka/consumer");
        for (KafkaBroker broker : zkUtils.getBrokerList()) {
            System.out.println(broker.getHost());
        }
        System.out.println(zkUtils.getLastCommit("kira", "test", 0));
        zkUtils.setLastCommit("kira", "test", 0, 5, false);
        System.out.println(zkUtils.getLastCommit("kira", "test", 0));
        zkUtils.close();
    }
}
