/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.kafka;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author zxc Mar 30, 2017 3:59:36 PM
 */
public class Producer {

    public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.180.73:9092,192.168.180.74:9092,192.168.180.75:9092");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        // File ctgsFile = new File("D:/baidu/data/categorys.txt");
        // String json = FileUtils.readFileToString(ctgsFile);

        boolean sync = true;
        String topic = "161_logstash_api";
        String key = null;
        String value = "{\"app\":\"environment\",\"clientIp\":\"222.73.199.34\",\"key\":\"bd0d8c183330\",\"localIp\":\"10.5.32.60\",\"param\":\" a2V5=YmQwZDhjMTgzMzMw Y2l0eQ===5YyX5Lqs\",\"time\":\"2016-01-12 14:42:51\",\"timeStamp\":1452580971325}";
        Integer partition = null;
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, partition, key, value);
        if (sync) {
            /* RecordMetadata recordMetadata = */producer.send(producerRecord)/* .get() */;
            /* System.out.println(JSON.toJSONString(recordMetadata.offset())); */
        } else {
            producer.send(producerRecord);
        }
        producer.close();
    }
}
