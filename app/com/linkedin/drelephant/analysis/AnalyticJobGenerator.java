package com.linkedin.drelephant.analysis;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.AuthenticationException;


/**
 * Provides AnalyticJobs that will yield to analysis results later. This class basically generates to-dos that could be
 * executed later.
 *
 */
public interface AnalyticJobGenerator {
  /**
   * Configures the provider instance
   *
   * @param configuration The Hadoop configuration object
   * @throws Exception
   */
  public void configure(Configuration configuration)
      throws IOException;

  /**
   * Provides a list of AnalyticJobs that should be calculated
   *
   * @return a list of AnalyticJobs
   * @throws IOException
   * @throws AuthenticationException
   */
  public List<AnalyticJob> fetchAnalyticJobs()
      throws IOException, AuthenticationException;

  /**
   * Add an AnalyticJob into retry list. Those jobs will be provided again via #fetchAnalyticJobs under
   * the generator's decision.
   *
   * @param job The job to add
   */
  public void addIntoRetries(AnalyticJob job);
}
