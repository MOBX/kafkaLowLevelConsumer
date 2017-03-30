/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.kafka.offset;

import java.io.Closeable;
import java.io.IOException;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * 基于Rocksdb实现的kafka offset提交
 * 
 * @author zxc Mar 30, 2017 3:59:36 PM
 */
public class RocksdbOffsetController implements OffsetController, Closeable {

    private Options options;
    private RocksDB db;
    private String  key;

    public RocksdbOffsetController(String dbPath, String group, String topic, int partition) throws RocksDBException {
        RocksDB.loadLibrary();
        options = new Options().setCreateIfMissing(true);
        db = RocksDB.open(options, dbPath);
        key = group + "-" + topic + "-" + partition;
    }

    @Override
    public boolean commitOffset(long offset) {
        try {
            db.put(key.getBytes(), String.valueOf(offset).getBytes());
            return true;
        } catch (RocksDBException e) {
            logger.error("RocksdbOffsetController commitOffset RocksDBException", e);
        }
        return false;
    }

    @Override
    public long getLastOffset() {
        try {
            byte[] value = db.get(key.getBytes());
            if (value == null) return -1;
            return Long.parseLong(new String(value));
        } catch (RocksDBException e) {
            logger.error("RocksdbOffsetController getLastOffset RocksDBException ", e);
        }
        return -1;
    }

    @Override
    public void close() throws IOException {
        if (db != null) db.close();
        if (options != null) options.dispose();
    }
}
