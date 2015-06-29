package com.linkedin.drelephant.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

public final class HadoopSystemContext {

  private static final String MAPREDUCE_FRAMEWORK_NAME_PROP = "mapreduce.framework.name";
  private static final String MAPRED_JOB_TRACKER_PROP = "mapred.job.tracker.http.address";
  private static final String YARN = "yarn";
  private static final String CLASSIC = "classic";

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
