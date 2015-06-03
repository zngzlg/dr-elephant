package com.linkedin.drelephant.analysis;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import java.io.IOException;

public final class HadoopSystemContext {
  private static final Logger logger = Logger.getLogger(HadoopSystemContext.class);
  public static long HDFS_BLOCK_SIZE = 64 * 1024 * 1024;
  public static final long DISK_READ_SPEED = 100 * 1024 * 1024;
  public static final int SHUFFLE_SORT_MAX_SAMPLE_SIZE = 50;

  private HadoopSystemContext() {
    // Empty on purpose
  }

  public static void load() {
    try {
      HDFS_BLOCK_SIZE = FileSystem.get(new Configuration()).getDefaultBlockSize(new Path("/"));
    } catch (IOException e) {
      logger.error("Error getting FS Block Size!", e);
    }

    logger.info("HDFS BLock size: " + HDFS_BLOCK_SIZE);
  }

  /**
   * Detect the current Hadoop version Dr Elephant is deployed for according to the current Hadoop Configuration.
   *
   * @return the current hadoop version (1 or 2 typically).
   */
  public static int getHadoopVersion() {
    // Important, new Configuration might not be timely loaded in Hadoop 1 environment
    Configuration hadoopConf = new JobConf();
    Map<String, String> vals = hadoopConf.getValByRegex(".*");
    String hadoopVersion = hadoopConf.get("mapreduce.framework.name");
    if (hadoopVersion == null) {
      if (hadoopConf.get("mapred.job.tracker.http.address") != null) {
        hadoopVersion = "classic";
      } else {
        throw new RuntimeException("Hadoop config error. No framework name provided.");
      }
    }

    if (hadoopVersion == null) {
      throw new RuntimeException("Cannot find Hadoop version information from corresponding Configuration object.");
    }
    if (hadoopVersion.equals("classic")) {
      return 1;
    } else if (hadoopVersion.equals("yarn")) {
      return 2;
    }

    throw new RuntimeException(
        "Cannot support hadoopVersion [" + hadoopVersion + "] other than classic or yarn currently.");
  }
}
