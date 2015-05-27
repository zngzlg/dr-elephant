package com.linkedin.drelephant.analysis;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.AuthenticationException;


/**
 * Provides analysis promises that will yield to analysis results finally.
 *
 */
public interface AnalysisProvider {
  /**
   * Configures the provider instance
   *
   * @param configuration The Hadoop configuration object
   * @throws Exception
   */
  public void configure(Configuration configuration)
      throws Exception;

  /**
   * Provides a list of analysis promises that should be calculated
   *
   * @return a list of analysis promises
   * @throws IOException
   * @throws AuthenticationException
   */
  public List<AnalysisPromise> fetchPromises()
      throws IOException, AuthenticationException;

  /**
   * Add an analysis promise into retry list. Those promises will be provided again via #fetchPromises under
   * the analysis provider's decision.
   *
   * @param promise The promise to add
   */
  public void addIntoRetries(AnalysisPromise promise);
}
