package com.linkedin.drelephant.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;

public class Constants {
    private static final Logger logger = Logger.getLogger(Constants.class);
    public static final long HDFS_BLOCK_SIZE;
    public static final long DISK_READ_SPEED = 100 * 1024 * 1024;

    static {
        long block_size = 64 * 1024 * 1024;

        try {
            block_size = FileSystem.get(new Configuration()).getDefaultBlockSize(new Path("/"));
        } catch (IOException e) {
            logger.error("Error getting FS Block Size!", e);
        }

        HDFS_BLOCK_SIZE = block_size;
    }

    public static void load() {
        logger.info("HDFS BLock size: " + HDFS_BLOCK_SIZE);
    }
}
