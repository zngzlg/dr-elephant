package com.linkedin.drelephant;

import com.linkedin.drelephant.hadoop.HadoopJobData;
import org.apache.hadoop.security.authentication.client.AuthenticationException;

import java.io.IOException;
import java.util.List;


/**
 * The interface to define common methods for each fetcher
 * We have different fetcher implementation for hadoop 1.x and hadoop 2.x
 */
public interface ElephantFetcher {

  /**
   * This method is called in Runner's main or each executor thread, before calling other fetcher methods
   * in order to initialize thread local variables for the thread.
   * @param a thread identifier of Runner's main/executor thread
   */
  public void init(int threadId) throws IOException;

  /**
   * This method is called periodically in main thread to fetch a list of jobs.
   * @return A list of Hadoop jobs (only job id, no data)
   */
  public List<HadoopJobData> fetchJobList() throws IOException, AuthenticationException;

  /**
   * This method is called in executor thread to fetch job data before calling analyzer method for the job
   * @param The job to fetch data for
   */
  public void fetchJobData(HadoopJobData jobData) throws IOException, AuthenticationException;

  /**
   * This method is called in executor thread to report if this job is successfully fetched and analyzed
   * Failed Job will be fetched again by fetchJobList()
   * @param The processed job
   * @param Whether the job is successfully fetched and analyzed
   */
  public void finishJob(HadoopJobData jobData, boolean success);
}
