package com.linkedin.drelephant.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;

public class Constants {
    private static final Logger logger = Logger.getLogger(Constants.class);
    public static long HDFS_BLOCK_SIZE = 64 * 1024 * 1024;
    public static final long DISK_READ_SPEED = 100 * 1024 * 1024;
    public static final int SHUFFLE_SORT_MAX_SAMPLE_SIZE = 50;

    public static void load() {
        try {
            HDFS_BLOCK_SIZE = FileSystem.get(new Configuration()).getDefaultBlockSize(new Path("/"));
        } catch (IOException e) {
            logger.error("Error getting FS Block Size!", e);
        }

        logger.info("HDFS BLock size: " + HDFS_BLOCK_SIZE);
    }
}
