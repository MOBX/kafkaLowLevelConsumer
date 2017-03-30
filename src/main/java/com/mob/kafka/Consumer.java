/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.kafka;

import java.nio.ByteBuffer;
import java.rmi.server.ServerNotActiveException;
import java.util.*;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import org.apache.kafka.common.errors.OffsetOutOfRangeException;

import com.lamfire.logger.Logger;
import com.mob.kafka.support.ConsumerConfig;
import com.mob.kafka.support.ConsumerRecord;

/**
 * @author zxc Mar 30, 2017 3:59:36 PM
 */
public class Consumer {

    private static final Logger LOGGER         = Logger.getLogger(Consumer.class);

    private List<String>        replicaBrokers = new ArrayList<String>();
    private ConsumerConfig      config;

    private Consumer() {
        replicaBrokers = new ArrayList<String>();
    }

    public Consumer(ConsumerConfig config) {
        this();
        this.config = config;
    }

    public SimpleConsumer getSimpleConsumer(String leadBroker) throws ServerNotActiveException {
        if (leadBroker != null && !leadBroker.isEmpty()) {
            return new SimpleConsumer(leadBroker, config.getPort(), config.getConnectionTimeout(), config.getBufferSize(), config.getClientId());
        }
        PartitionMetadata metadata = findLeader(config.getSeedBrokers(), config.getPort(), config.getTopicName(), config.getPartition());
        if (metadata == null) {
            LOGGER.error("Can't find metadata for Topic and Partition. Exiting");
            throw new ServerNotActiveException("Can't find metadata for Topic and Partition.");
        }
        if (metadata.leader() == null) {
            LOGGER.error("Can't find Leader for Topic and Partition. Exiting");
            throw new ServerNotActiveException("Can't find Leader for Topic and Partition. Exiting");
        }
        leadBroker = metadata.leader().host();
        return new SimpleConsumer(leadBroker, config.getPort(), config.getConnectionTimeout(), config.getBufferSize(), config.getClientId());
    }

    public List<ConsumerRecord> run(long readOffset) throws ServerNotActiveException {
        // find the meta data about the topic and partition we are interested in

        // PartitionMetadata metadata = findLeader(seedBrokers, config.getPort(), config.getTopicName(), partition);
        // if (metadata == null) {
        // LOGGER.error("Can't find metadata for Topic and Partition. Exiting");
        // throw new ServerNotActiveException("Can't find metadata for Topic and Partition.");
        // }
        // if (metadata.leader() == null) {
        // LOGGER.error("Can't find Leader for Topic and Partition. Exiting");
        // throw new ServerNotActiveException("Can't find Leader for Topic and Partition. Exiting");
        // }
        // String leadBroker = metadata.leader().host();
        // String clientName = config.getClientName() + "_" + config.getTopicName() + "_" + partition;

        SimpleConsumer consumer = getSimpleConsumer(null);
        if (readOffset == 0) {
            readOffset = getFirstOffset(consumer, config.getTopicName(), config.getPartition(), config.getClientId());
        }

        List<ConsumerRecord> recordList = new ArrayList<ConsumerRecord>();

        ByteBuffer keyBuffer = null, payloadBuffer = null;
        FetchRequest req = new FetchRequestBuilder().clientId(config.getClientId()).addFetch(config.getTopicName(), config.getPartition(), readOffset, config.getFetchSize()).build();
        FetchResponse fetchResponse = consumer.fetch(req);

        if (fetchResponse.hasError()) {
            // Something went wrong!
            short code = fetchResponse.errorCode(config.getTopicName(), config.getPartition());
            LOGGER.error("Error fetching data from the Broker:" + consumer.host() + " Reason: " + code);

            if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                // We asked for an invalid offset. For simple case ask for the
                // last element to reset
                // readOffset = getOffset(consumer,config.getTopicName(),
                // partition, kafka.api.OffsetRequest.LatestTime(), clientName);
                throw new OffsetOutOfRangeException("Offset = " + readOffset);

            }
            String oldLeader = consumer.host();
            consumer.close();
            consumer = null;
            consumer = getSimpleConsumer(findNewLeader(oldLeader, config.getTopicName(), config.getPartition(), config.getPort()));
        }

        for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(config.getTopicName(), config.getPartition())) {
            long currentOffset = messageAndOffset.offset();
            if (currentOffset < readOffset) {
                LOGGER.error("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                continue;
            }
            readOffset = messageAndOffset.nextOffset();
            keyBuffer = messageAndOffset.message().key();
            payloadBuffer = messageAndOffset.message().payload();
            byte[] keys = null, values = null;
            if (keyBuffer != null) {
                keys = new byte[keyBuffer.limit()];
                keyBuffer.get(keys);
            }
            values = new byte[payloadBuffer.limit()];
            payloadBuffer.get(values);
            recordList.add(new ConsumerRecord(currentOffset, keys, values));

        }

        if (consumer != null) consumer.close();
        return recordList;
    }

    public ConsumerConfig getConfig() {
        return config;
    }

    public long getLastOffset() throws ServerNotActiveException {
        SimpleConsumer consumer = getSimpleConsumer(null);
        long lastOffset = getLastOffset(consumer, config.getTopicName(), config.getPartition(), config.getClientName());
        if (consumer != null) consumer.close();
        return lastOffset;
    }

    public long getFirstOffset() throws ServerNotActiveException {
        SimpleConsumer consumer = getSimpleConsumer(null);
        long firstOffset = getFirstOffset(consumer, config.getTopicName(), config.getPartition(), config.getClientName());
        if (consumer != null) consumer.close();
        return firstOffset;
    }

    public static long getFirstOffset(SimpleConsumer consumer, String topic, int partition, String clientName) {
        return getOffset(consumer, topic, partition, kafka.api.OffsetRequest.EarliestTime(), clientName);
    }

    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition, String clientName) {
        return getOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(), clientName);
    }

    public static long getOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            LOGGER.error("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
            return -1;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    private String findNewLeader(String oldLeader, String topic, int partition, int port) throws ServerNotActiveException {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            String[] brokersArray = new String[replicaBrokers.size()];
            PartitionMetadata metadata = findLeader(replicaBrokers.toArray(brokersArray), port, topic, partition);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                // first time through if the leader hasn't changed give
                // ZooKeeper a second to recover
                // second time, assume the broker did recover before failover,
                // or it was a non-Broker issue
                //
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        LOGGER.error("Unable to find new leader after Broker failure. Exiting");
        throw new ServerNotActiveException("Unable to find new leader after Broker failure. Exiting");
    }

    private PartitionMetadata findLeader(String[] seedBrokers, int port, String topic, int partition) {
        PartitionMetadata returnMetaData = null;
        loop: for (String seed : seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed, port, config.getFetchSize(), config.getBufferSize(), "leaderLookup");
                List<String> topics = Collections.singletonList(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == partition) {
                            returnMetaData = part;
                            break loop;
                        }
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Error communicating with Broker [" + seed + "] to find Leader for [" + topic + ", " + partition + "] Reason: " + e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        if (returnMetaData != null) {
            replicaBrokers.clear();
            for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
                replicaBrokers.add(replica.host());
            }
        }
        return returnMetaData;
    }

    public static void main(String[] args) {
        int partition = 1;
        List<String> seeds = new ArrayList<String>();
        seeds.add("192.168.180.73");
        seeds.add("192.168.180.74");
        seeds.add("192.168.180.75");
        Consumer example = new Consumer(new ConsumerConfig("kira_test", "gold_topic", (String[]) seeds.toArray(new String[3]), partition));

        // Consumer example1 = new Consumer(new ConsumerConfig("kira_test", "161_logstash_api", (String[])
        // seeds.toArray(new String[3]), 1));
        //
        // Consumer example2 = new Consumer(new ConsumerConfig("kira_test", "161_logstash_api", (String[])
        // seeds.toArray(new String[3]), 2));
        try {
            long lastOffset = example.getLastOffset();
            long cursor = 0;
            while (true) {
                Thread.sleep(1000);
                while (cursor < lastOffset) {
                    List<ConsumerRecord> list = example.run(cursor);
                    for (ConsumerRecord cr : list) {
                        System.out.println(cr.toString());
                        cursor++;
                    }
                    // lastOffset = example.getLastOffset();
                    System.out.println("===");
                }
                lastOffset = example.getLastOffset();
            }
        } catch (Exception e) {
            System.out.println("Oops:" + e);
            e.printStackTrace();
        }
    }

}
