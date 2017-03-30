/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.kafka;

/**
 * @author zxc Mar 30, 2017 3:46:05 PM
 */
public interface IService {

    public void run();

    public void reload();

    public void shutdown();
}
