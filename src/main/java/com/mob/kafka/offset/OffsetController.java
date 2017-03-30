/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.kafka.offset;

import com.lamfire.logger.Logger;

/**
 * @author zxc Mar 30, 2017 3:59:36 PM
 */
public interface OffsetController {

    public static final Logger logger = Logger.getLogger(OffsetController.class);

    public boolean commitOffset(long offset);

    public long getLastOffset();
}
