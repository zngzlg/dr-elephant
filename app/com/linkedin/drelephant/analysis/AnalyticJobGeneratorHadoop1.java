/*
 * Copyright 2015 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.linkedin.drelephant.analysis;

import com.linkedin.drelephant.ElephantContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import model.JobResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;


/**
 * This class provides a AnalyticJob list to be fetched/analyzed in Hadoop 1 environment.
 *
 */
public class AnalyticJobGeneratorHadoop1 implements AnalyticJobGenerator {
  private static final Logger logger = Logger.getLogger(AnalyticJobGeneratorHadoop1.class);
  private JobClient _jobClient;

  private boolean _firstRun = true;
  private Set<String> _previousJobs;

  private final Queue<AnalyticJob> _retryQueue = new ConcurrentLinkedQueue<AnalyticJob>();
  private static final String ONLY_SUPPORTED_TYPE_NAME = "MAPREDUCE";
  private ApplicationType _onlySupportedType;

  @Override
  public void configure(Configuration configuration)
      throws IOException {
    _jobClient = new JobClient(new JobConf(configuration));

    _onlySupportedType = ElephantContext.instance().getApplicationTypeForName(ONLY_SUPPORTED_TYPE_NAME);
    if (_onlySupportedType == null) {
      throw new RuntimeException("Cannot configure the analysis provider, " + ONLY_SUPPORTED_TYPE_NAME
          + " application type is not supported.");
    }
  }

  @Override
  public List<AnalyticJob> fetchAnalyticJobs()
      throws IOException, AuthenticationException {
    JobStatus[] result = _jobClient.getAllJobs();

    if (result == null) {
      throw new IOException("Error fetching joblist from jobtracker.");
    }

    // Get all completed jobs from jobtracker
    Set<String> successJobs = filterSuccessfulJobs(result);

    // Filter out newly completed jobs
    Set<String> todoJobs = filterPreviousJobs(successJobs);

    // Add newly completed jobs to return list
    List<AnalyticJob> jobList = new ArrayList<AnalyticJob>();
    for (String jobId : todoJobs) {
      RunningJob job = _jobClient.getJob(jobId);
      JobStatus jobStatus = job.getJobStatus();

      AnalyticJob analyticJob = new AnalyticJob();
      long startTime = jobStatus.getStartTime();

      analyticJob.setAppId(jobId).setAppType(_onlySupportedType).setJobId(jobId).setUser(jobStatus.getUsername())
          .setName(job.getJobName()).setTrackingUrl(job.getTrackingURL()).setStartTime(startTime);
      // Note:
      //     In Hadoop-1 the getFinishTime() call is not there. Unfortunately, calling this method does not
      //     result in a compile error, neither a MethodNotFound exception at run time. The program just hangs.
      //     Since we don't have metrics (the only consumer of finish time as of now) in Hadoop-1, set the finish time
      //     to be the same as start time.
      analyticJob.setFinishTime(startTime);

      jobList.add(analyticJob);
    }
    // Append promises from the retry queue at the end of the list
    while (!_retryQueue.isEmpty()) {
      jobList.add(_retryQueue.poll());
    }

    return jobList;
  }

  @Override
  public void addIntoRetries(AnalyticJob promise) {
    _retryQueue.add(promise);
  }

  // Return a set of succeeded job ids
  private Set<String> filterSuccessfulJobs(JobStatus[] jobs) {
    HashSet<String> successJobs = new HashSet<String>();
    for (JobStatus job : jobs) {
      if (job.getRunState() == JobStatus.SUCCEEDED && job.isJobComplete()) {
        successJobs.add(job.getJobID().toString());
      }
    }
    return successJobs;
  }

  private Set<String> filterPreviousJobs(Set<String> jobs) {
    // On first run, check each job against DB
    if (_firstRun) {
      logger.info("Checking database ...... ");
      Set<String> newJobs = new HashSet<String>();
      for (String jobId : jobs) {
        JobResult prevResult = JobResult.find.byId(jobId);
        if (prevResult == null) {
          // Job not found, add to new jobs list
          newJobs.add(jobId);
        }
      }
      _previousJobs = jobs;
      _firstRun = false;
      logger.info("Database check completed");
      return newJobs;
    } else {
      Set<String> tempPrevJobs = new HashSet<String>(jobs);
      // Leave only the newly completed jobs
      jobs.removeAll(_previousJobs);
      _previousJobs = tempPrevJobs;
      return jobs;
    }
  }
}
