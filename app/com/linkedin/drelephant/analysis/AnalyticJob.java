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
import com.linkedin.drelephant.util.InfoExtractor;
import com.linkedin.drelephant.util.Utils;
import java.util.ArrayList;
import java.util.List;
import model.JobHeuristicResult;
import model.JobResult;
import org.apache.log4j.Logger;


/**
 * This class wraps some basic meta data of a completed application run (notice that the information is generally the
 * same regardless of hadoop versions and application types), and then promises to return the analyzed result later.
 */
public class AnalyticJob {
  private static final Logger logger = Logger.getLogger(AnalyticJob.class);

  private static final String UNKNOWN_JOB_TYPE = "Unknown";   // The default job type when the data matches nothing.
  private static final int _RETRY_LIMIT = 3;                  // Number of times a job needs to be tried before dropping

  private int _retries = 0;
  private ApplicationType _type;
  private String _appId;
  private String _jobId;
  private String _name;
  private String _user;
  private String _trackingUrl;
  private long _startTime;
  private long _finishTime;

  /**
   * Returns the application type
   * E.g., Mapreduce or Spark
   *
   * @return The application type
   */
  public ApplicationType getAppType() {
    return _type;
  }

  /**
   * Set the application type of this job.
   *
   * @param type The Application type
   * @return The analytic job
   */
  public AnalyticJob setAppType(ApplicationType type) {
    _type = type;
    return this;
  }

  /**
   * Set the application id of this job
   *
   * @param appId The application id of the job obtained resource manager
   * @return The analytic job
   */
  public AnalyticJob setAppId(String appId) {
    _appId = appId;
    return this;
  }

  /**
   * Set the id of the job
   * jobId is the appId with the prefix 'application_' replaced by 'job_'
   *
   * @param jobId The job id
   * @return The analytic job
   */
  public AnalyticJob setJobId(String jobId) {
    _jobId = jobId;
    return this;
  }

  /**
   * Set the name of the analytic job
   *
   * @param name
   * @return The analytic job
   */
  public AnalyticJob setName(String name) {
    _name = name;
    return this;
  }

  /**
   * Sets the user who ran the job
   *
   * @param user The username of the user
   * @return The analytic job
   */
  public AnalyticJob setUser(String user) {
    _user = user;
    return this;
  }

  /**
   * Sets the start time of the job
   * Start time is the time at which the job was submitted by the resource manager
   *
   * @param startTime
   * @return The analytic job
   */
  public AnalyticJob setStartTime(long startTime) {
    _startTime = startTime;
    return this;
  }

  /**
   * Sets the finish time of the job
   *
   * @param finishTime
   * @return The analytic job
   */
  public AnalyticJob setFinishTime(long finishTime) {
    _finishTime = finishTime;
    return this;
  }

  /**
   * Returns the application id
   *
   * @return The analytic job
   */
  public String getAppId() {
    return _appId;
  }

  /**
   * Returns the job id
   *
   * @return The job id
   */
  public String getJobId() {
    return _jobId;
  }

  /**
   * Returns the name of the analytic job
   *
   * @return the analytic job's name
   */
  public String getName() {
    return _name;
  }

  /**
   * Returns the user who ran the job
   *
   * @return The user who ran the analytic job
   */
  public String getUser() {
    return _user;
  }

  /**
   * Returns the time at which the job was submitted by the resource manager
   *
   * @return The start time
   */
  public long getStartTime() {
    return _startTime;
  }

  /**
   * Returns the finish time of the job.
   *
   * @return The finish time
   */
  public long getFinishTime() {
    return _finishTime;
  }

  /**
   * Returns the tracking url of the job
   *
   * @return The tracking url in resource manager
   */
  public String getTrackingUrl() {
    return _trackingUrl;
  }

  /**
   * Sets the tracking url for the job
   *
   * @param trackingUrl The url to track the job
   * @return The analytic job
   */
  public AnalyticJob setTrackingUrl(String trackingUrl) {
    _trackingUrl = trackingUrl;
    return this;
  }

  /**
   * Returns the analysed JobResult that could be directly serialized into DB.
   *
   * This method fetches the data using the appropriate application fetcher, runs all the heuristics on them and
   * loads it into the JobResult model.
   *
   * @throws Exception if the analysis process encountered a problem.
   * @return the analysed JobResult
   */
  public JobResult getAnalysis() throws Exception {
    ElephantFetcher fetcher = ElephantContext.instance().getFetcherForApplicationType(getAppType());
    HadoopApplicationData data = fetcher.fetchData(this);

    // Run all heuristics over the fetched data
    List<HeuristicResult> analysisResults = new ArrayList<HeuristicResult>();
    if (data == null || data.isEmpty()) {
      // Example: a MR job has 0 mappers and 0 reducers
      logger.info("No Data Received for analytic job: " + getAppId());
      analysisResults.add(HeuristicResult.NO_DATA);
    } else {
      List<Heuristic> heuristics = ElephantContext.instance().getHeuristicsForApplicationType(getAppType());
      for (Heuristic heuristic : heuristics) {
        HeuristicResult result = heuristic.apply(data);
        if (result != null) {
          analysisResults.add(result);
        }
      }
    }

    JobType jobType = ElephantContext.instance().matchJobType(data);
    String jobTypeName = jobType == null ? UNKNOWN_JOB_TYPE : jobType.getName();

    // Load job information
    JobResult result = new JobResult();
    result.jobId = Utils.getJobIdFromApplicationId(getAppId());
    result.url = getTrackingUrl();
    result.username = getUser();
    result.startTime = getStartTime();
    result.analysisTime = getFinishTime();
    result.jobName = getName();
    result.jobType = jobTypeName;

    // Truncate long names
    if (result.jobName.length() > 100) {
      result.jobName = result.jobName.substring(0, 97) + "...";
    }

    // Load Job Heuristic information
    result.heuristicResults = new ArrayList<JobHeuristicResult>();
    Severity worstSeverity = Severity.NONE;
    for (HeuristicResult heuristicResult : analysisResults) {
      JobHeuristicResult detail = new JobHeuristicResult();
      detail.analysisName = heuristicResult.getAnalysis();
      detail.data = heuristicResult.getDetailsCSV();
      detail.dataColumns = heuristicResult.getDetailsColumns();
      detail.severity = heuristicResult.getSeverity();
      if (detail.dataColumns < 1) {
        detail.dataColumns = 1;
      }
      result.heuristicResults.add(detail);
      worstSeverity = Severity.max(worstSeverity, detail.severity);
    }
    result.severity = worstSeverity;

    // Retrieve Azkaban execution, flow and jobs URLs from jobData and store them into result.
    InfoExtractor.retrieveURLs(result, data);

    return result;
  }

  /**
   * Indicate this promise should retry itself again.
   *
   * @return true if should retry, else false
   */
  public boolean retry() {
    return (_retries++) < _RETRY_LIMIT;
  }
}
