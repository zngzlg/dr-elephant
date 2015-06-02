package com.linkedin.drelephant.spark;

import java.util.Properties;


/**
 * This data class holds Spark environment data (Spark properties, JVM properties and etc.)
 */
public class SparkEnvironmentData {
  private final Properties _sparkProperties;
  private final Properties _systemProperties;

  public SparkEnvironmentData() {
    _sparkProperties = new Properties();
    _systemProperties = new Properties();
  }

  public void addSparkProperty(String key, String value) {
    _sparkProperties.put(key, value);
  }

  public void addSystemProperty(String key, String value) {
    _systemProperties.put(key, value);
  }

  public String getSparkProperty(String key) {
    return _sparkProperties.getProperty(key);
  }

  public String getSparkProperty(String key, String defaultValue) {
    String val = getSparkProperty(key);
    if (val == null) {
      return defaultValue;
    }
    return val;
  }

  public String getSystemProperty(String key) {
    return _systemProperties.getProperty(key);
  }

  public Properties getSparkProperties() {
    return _sparkProperties;
  }

  public Properties getSystemProperties() {
    return _systemProperties;
  }

  @Override
  public String toString() {
    return _sparkProperties.toString() + "\n\n\n" + _systemProperties.toString();
  }
}
