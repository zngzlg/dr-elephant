package com.linkedin.drelephant.analysis;

import java.util.Properties;


/**
 * This interface indicates that a class is holding the information of a Hadoop application
 */
public interface HadoopApplicationData {

  /**
   * Returns the unique id to identify an application run. This id could be application id or job id according to
   * different applications, as long as this is unique.
   *
   * @return the uid
   */
  public String getUid();

  /**
   * Returns the configuration of an application.
   *
   * @return A java Properties that contains the application configuration
   */
  public Properties getConf();
}
