/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.kafka;

import java.io.*;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.lamfire.logger.Logger;

/**
 * @author zxc Mar 30, 2017 3:59:36 PM
 */
public class FileProducer implements IService {

    static Logger                 LOGGER = Logger.getLogger(FileProducer.class);

    private File                  file;
    private String                topicName;
    private String                partition;
    KafkaProducer<String, String> producer;

    FileProducer(String[] args) throws org.apache.commons.cli.ParseException {
        init(args);
    }

    void init(String[] args) throws org.apache.commons.cli.ParseException {
        CommandLineParser parser = new BasicParser();
        Options options = new Options();
        options.addOption("s", "server", true, "bootstrap.servers");
        options.addOption("f", "file", true, "file path & only support gzip file");
        options.addOption("t", "topicName", true, "topic name");
        options.addOption("p", "partition", true, "partition number or 'all'");

        CommandLine commandLine = parser.parse(options, args);

        if (!commandLine.hasOption("s") || !commandLine.hasOption("f") || !commandLine.hasOption("t") || !commandLine.hasOption("p")) {
            LOGGER.error("args error");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Use age", options);
            System.exit(0);
        }
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, commandLine.getOptionValue("s"));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<String, String>(props);
        String filePath = commandLine.getOptionValue("f");
        file = new File(filePath);
        if (!file.getName().endsWith("gz")) {
            LOGGER.error("just support gzip file");
            throw new IllegalArgumentException();
        }
        topicName = commandLine.getOptionValue("t");
        partition = commandLine.getOptionValue("p");
    }

    @Override
    public void run() {
        BufferedReader br = null;
        int lineNum = 0;
        ProducerRecord<String, String> producerRecord = null;
        try {
            br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
            String line = null;
            while ((line = br.readLine()) != null) {
                if (partition != null && "all".equals(partition.toLowerCase())) {
                    producerRecord = new ProducerRecord<String, String>(topicName, line);
                } else {
                    producerRecord = new ProducerRecord<String, String>(topicName, Integer.parseInt(partition), "", line);
                }
                producer.send(producerRecord);
                lineNum++;
            }
        } catch (IOException e) {
            LOGGER.error("io exception lineNum : " + lineNum, e);
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                LOGGER.error("BufferedReader io exception", e);
            }

        }
        producer.close();
        LOGGER.info("produce over");
    }

    @Override
    public void reload() {

    }

    @Override
    public void shutdown() {
        if (producer != null) producer.close();
    }

    public static void main(String[] args) throws org.apache.commons.cli.ParseException {
        FileProducer fileProducer = new FileProducer(args);
        fileProducer.run();
    }
}
