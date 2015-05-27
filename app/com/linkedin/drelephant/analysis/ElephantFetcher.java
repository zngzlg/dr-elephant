package com.linkedin.drelephant.analysis;

import java.io.IOException;


/**
 * The interface to define common methods for each fetcher.
 *
 * There would be a different fetcher implementation given a different Hadoop version and a different application type.
 */
public interface ElephantFetcher<T extends HadoopApplicationData> {

  /**
   * This method is called in Runner's main or each executor thread, before calling other fetcher methods
   * in order to initialize thread local variables for the thread.
   * @param threadId a thread identifier of Runner's main/executor thread
   */
  public void init(int threadId) throws IOException;

  /**
   * Given an application id, fetches the data object
   *
   * @param appId the application id
   * @return the fetched data
   * @throws Exception
   */
  public T fetchData(String appId) throws Exception;
}
