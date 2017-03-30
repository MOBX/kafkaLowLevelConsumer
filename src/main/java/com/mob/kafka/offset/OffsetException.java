/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.kafka.offset;

/**
 * @author zxc Mar 30, 2017 4:05:36 PM
 */
public class OffsetException extends RuntimeException {

    private static final long serialVersionUID = 573988050149554734L;

    public OffsetException() {
    }

    public OffsetException(String paramString) {
        super(paramString);
    }
}
