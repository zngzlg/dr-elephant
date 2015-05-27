package com.linkedin.drelephant.spark;

import com.linkedin.drelephant.analysis.ApplicationType;
import java.util.Properties;


/**
 * This is a pseudo local implementation of SparkApplicationData interface, supposed to be used for test purpose.
 *
 * @author yizhou
 */
public class MockSparkApplicationData implements SparkApplicationData {
  private final SparkGeneralData _sparkGeneralData;
  private final SparkEnvironmentData _sparkEnvironmentData;
  private final SparkExecutorData _sparkExecutorData;
  private final SparkJobProgressData _sparkJobProgressData;
  private final SparkStorageData _sparkStorageData;

  public MockSparkApplicationData() {
    _sparkGeneralData = new SparkGeneralData();
    _sparkEnvironmentData = new SparkEnvironmentData();
    _sparkExecutorData = new SparkExecutorData();
    _sparkJobProgressData = new SparkJobProgressData();
    _sparkStorageData = new SparkStorageData();
  }

  @Override
  public SparkGeneralData getGeneralData() {
    return _sparkGeneralData;
  }

  @Override
  public SparkEnvironmentData getEnvironmentData() {
    return _sparkEnvironmentData;
  }

  @Override
  public SparkExecutorData getExecutorData() {
    return _sparkExecutorData;
  }

  @Override
  public SparkJobProgressData getJobProgressData() {
    return _sparkJobProgressData;
  }

  @Override
  public SparkStorageData getStorageData() {
    return _sparkStorageData;
  }

  @Override
  public Properties getConf() {
    return getEnvironmentData().getSparkProperties();
  }

  @Override
  public String getUid() {
    return getGeneralData().getApplicationId();
  }
}
