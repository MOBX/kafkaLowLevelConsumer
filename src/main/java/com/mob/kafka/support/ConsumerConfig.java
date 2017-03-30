/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.kafka.support;

/**
 * @author zxc Mar 30, 2017 3:59:36 PM
 */
public class ConsumerConfig {

    public static final String KAFKA_CONSUMER_CLINET_NAME           = "kafka.consumer.client.name";
    public static final String KAFKA_CONSUMER_CONNECTION_TIMEOUT_MS = "kafka.consumer.connection.timeout.ms";
    public static final String KAFKA_CONSUMER_BUFFER_SIZE           = "kafka.consumer.buffer.size";
    public static final String KAFKA_CONSUMER_FETCH_SIZE            = "kafka.consumer.fetch.size";
    public static final String KAFKA_PORT                           = "kafka.port";
    public static final String KAFKA_HOSTS                          = "kafka.hosts";
    public static final String KAFKA_TOPIC_NAME                     = "kafka.topic.name";

    private int                connectionTimeout                    = 10000;
    private int                bufferSize                           = 64 * 1024;
    private int                fetchSize                            = 100000;
    private int                port                                 = 9092;
    private String             clientName;
    private String             topicName;
    private String[]           seedBrokers;
    private int                partition;

    public ConsumerConfig(String clientName, String topicName, String[] seedBrokers, int partition) {
        this.clientName = clientName;
        this.topicName = topicName;
        this.seedBrokers = seedBrokers;
        this.partition = partition;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public String getClientName() {
        return clientName;
    }

    public String getClientId() {
        return getClientName() + "_" + getTopicName() + "_" + getPartition();
    }

    public String getTopicName() {
        return topicName;
    }

    public String[] getSeedBrokers() {
        return seedBrokers;
    }

    public int getPartition() {
        return partition;
    }
}
