package com.linkedin.drelephant.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import java.io.IOException;

public final class HadoopSystemContext {
  private static final Logger logger = Logger.getLogger(HadoopSystemContext.class);

  private static final String MAPREDUCE_FRAMEWORK_NAME_PROP = "mapreduce.framework.name";
  private static final String MAPRED_JOB_TRACKER_PROP = "mapred.job.tracker.http.address";
  private static final String YARN = "yarn";
  private static final String CLASSIC = "classic";

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
   * Detect if the current Hadoop environment is 1.x
   *
   * @return true if it is Hadoop 1 env, else false
   */
  public static boolean isHadoop1Env() {
    Configuration hadoopConf = new JobConf();
    String hadoopVersion = hadoopConf.get(MAPREDUCE_FRAMEWORK_NAME_PROP);
    if (hadoopVersion == null) {
      return hadoopConf.get(MAPRED_JOB_TRACKER_PROP) != null;
    }
    return hadoopVersion.equals(CLASSIC);
  }

  /**
   * Detect if the current Hadoop environment is 2.x
   *
   * @return true if it is Hadoop 2 env, else false
   */
  public static boolean isHadoop2Env() {
    Configuration hadoopConf = new JobConf();
    String hadoopVersion = hadoopConf.get(MAPREDUCE_FRAMEWORK_NAME_PROP);
    return hadoopVersion != null && hadoopVersion.equals(YARN);
  }

  /**
   * Check if a Hadoop version matches the current Hadoop environment
   *
   * @param majorVersion the major version number of hadoop
   * @return true if we have a major version match else false
   */
  public static boolean matchCurrentHadoopVersion(int majorVersion) {
    return majorVersion == 2 && isHadoop2Env() || majorVersion == 1 && isHadoop1Env();
  }
}
