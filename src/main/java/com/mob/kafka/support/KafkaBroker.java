/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.kafka.support;

/**
 * @author zxc Mar 30, 2017 3:59:36 PM
 */
public class KafkaBroker {

    private int    version;
    private String host;
    private int    port;
    private int    jmx_port;

    public int getJmx_port() {
        return jmx_port;
    }

    public void setJmx_port(int jmx_port) {
        this.jmx_port = jmx_port;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
