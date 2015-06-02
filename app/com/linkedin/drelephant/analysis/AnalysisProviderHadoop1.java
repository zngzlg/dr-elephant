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
 * This class provides a future list to be fetched/analyzed in Hadoop 1 environment.
 *
 */
public class AnalysisProviderHadoop1 implements AnalysisProvider {
  private static final Logger logger = Logger.getLogger(AnalysisProviderHadoop1.class);
  private JobClient _jobClient;

  private boolean _firstRun = true;
  private Set<String> _previousJobs;

  private final Queue<AnalysisPromise> _retryQueue = new ConcurrentLinkedQueue<AnalysisPromise>();
  private static final String ONLY_SUPPORTED_TYPE_NAME = "MAPREDUCE";
  private ApplicationType _onlySupportedType;

  @Override
  public void configure(Configuration configuration)
      throws Exception {
    _jobClient = new JobClient(new JobConf(configuration));

    _onlySupportedType = ElephantContext.instance().getApplicationType(ONLY_SUPPORTED_TYPE_NAME);
    if (_onlySupportedType == null) {
      throw new RuntimeException("Cannot configure the analysis provider, " + ONLY_SUPPORTED_TYPE_NAME
          + " application type is not supported.");
    }
  }

  @Override
  public List<AnalysisPromise> fetchPromises()
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
    List<AnalysisPromise> jobList = new ArrayList<AnalysisPromise>();
    for (String jobId : todoJobs) {
      RunningJob job = _jobClient.getJob(jobId);
      JobStatus jobStatus = job.getJobStatus();

      AnalysisPromise promise = new AnalysisPromise();

      promise.setAppId(jobId);
      promise.setAppType(_onlySupportedType);
      promise.setJobId(jobId);
      promise.setUser(jobStatus.getUsername());
      promise.setName(job.getJobName());
      promise.setTrackingUrl(job.getTrackingURL());

      long startTime = jobStatus.getStartTime();
      promise.setStartTime(startTime);
      // Note:
      //     In Hadoop-1 the getFinishTime() call is not there. Unfortunately, calling this method does not
      //     result in a compile error, neither a MethodNotFound exception at run time. The program just hangs.
      //     Since we don't have metrics (the only consumer of finish time as of now) in Hadoop-1, set the finish time
      //     to be the same as start time.
      promise.setFinishTime(startTime);

      jobList.add(promise);
    }
    // Append promises from the retry queue at the end of the list
    while (!_retryQueue.isEmpty()) {
      jobList.add(_retryQueue.poll());
    }

    return jobList;
  }

  @Override
  public void addIntoRetries(AnalysisPromise promise) {
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
