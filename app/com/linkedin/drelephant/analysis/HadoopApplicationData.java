package com.linkedin.drelephant.analysis;

import java.util.Properties;


/**
 * This interface indicates that a class is holding the information of a Hadoop application
 */
public interface HadoopApplicationData {

  /**
   * Returns the unique id to identify an application run.
   *
   * @return the id
   */
  public String getAppId();

  /**
   * Returns the configuration of an application.
   *
   * @return A java Properties that contains the application configuration
   */
  public Properties getConf();

  /**
   * Returns the application type this data is for
   *
   * @return the application type
   */
  public ApplicationType getApplicationType();

  /**
   * Indicate if the data holder is actually empty (nothing is set up).
   *
   * @return true if the data holder is empty else false
   */
  public boolean isEmpty();
}
