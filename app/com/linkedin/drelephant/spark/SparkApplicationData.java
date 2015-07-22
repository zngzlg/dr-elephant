package com.linkedin.drelephant.spark;

import com.linkedin.drelephant.analysis.HadoopApplicationData;


/**
 * This holds a collection of all SparkApplicationData
 *
 */
public interface SparkApplicationData extends HadoopApplicationData {

  public boolean isThrottled();

  public SparkGeneralData getGeneralData();

  public SparkEnvironmentData getEnvironmentData();

  public SparkExecutorData getExecutorData();

  public SparkJobProgressData getJobProgressData();

  public SparkStorageData getStorageData();
}
