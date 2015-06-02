package com.linkedin.drelephant.analysis;

/**
 * The interface to define common methods for each fetcher.
 *
 * There would be a different fetcher implementation given a different Hadoop version and a different application type.
 */
public interface ElephantFetcher<T extends HadoopApplicationData> {

  /**
   * Given an application id, fetches the data object
   *
   * @param appId the application id
   * @return the fetched data
   * @throws Exception
   */
  public T fetchData(String appId)
      throws Exception;
}
