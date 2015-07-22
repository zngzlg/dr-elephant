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
 * same regardless of hadoop versions and application types), and then promises to return the analyzed result
 * later.
 */
public class AnalyticJob {
  private static final Logger logger = Logger.getLogger(AnalyticJob.class);
  // The defualt job type when the data matches nothing.
  private static final String UNKNOWN_JOB_TYPE = "Unknown";
  private static final int _RETRY_LIMIT = 3;

  private int _retries = 0;
  private ApplicationType _type;
  private String _appId;
  private String _jobId;
  private String _name;
  private String _user;
  private String _trackingUrl;
  private long _startTime;
  private long _finishTime;

  public ApplicationType getAppType() {
    return _type;
  }

  public AnalyticJob setAppType(ApplicationType type) {
    _type = type;
    return this;
  }

  public AnalyticJob setAppId(String appId) {
    _appId = appId;
    return this;
  }

  public AnalyticJob setJobId(String jobId) {
    _jobId = jobId;
    return this;
  }

  public AnalyticJob setName(String name) {
    _name = name;
    return this;
  }

  public AnalyticJob setUser(String user) {
    _user = user;
    return this;
  }

  public AnalyticJob setStartTime(long startTime) {
    _startTime = startTime;
    return this;
  }

  public AnalyticJob setFinishTime(long finishTime) {
    _finishTime = finishTime;
    return this;
  }

  public String getAppId() {
    return _appId;
  }

  public String getJobId() {
    return _jobId;
  }

  public String getName() {
    return _name;
  }

  public String getUser() {
    return _user;
  }

  public long getStartTime() {
    return _startTime;
  }

  public long getFinishTime() {
    return _finishTime;
  }

  public String getTrackingUrl() {
    return _trackingUrl;
  }

  public AnalyticJob setTrackingUrl(String trackingUrl) {
    _trackingUrl = trackingUrl;
    return this;
  }

  /**
   * Returns the analysed JobResult that could be directly serialized into DB.
   *
   * @throws Exception if the analysis process encountered a problem.
   * @return the analysed JobResult
   */
  public JobResult getAnalysis() throws Exception {
    ElephantFetcher fetcher = ElephantContext.instance().getFetcherForApplicationType(getAppType());

    HadoopApplicationData data = fetcher.fetchData(getAppId());

    List<HeuristicResult> analysisResults = new ArrayList<HeuristicResult>();

    if (data == null || data.isEmpty()) {
      logger.info("No Data Received for analytic job: " + getAppId());
      analysisResults.add(HeuristicResult.NO_DATA);
    } else {
      List<Heuristic> heuristics = ElephantContext.instance().getHeuristicsForApplicationType(getAppType());
      for (Heuristic heuristic : heuristics) {
        analysisResults.add(heuristic.apply(data));
      }
    }

    JobType jobType = ElephantContext.instance().matchJobType(data);
    String jobTypeName = jobType == null ? UNKNOWN_JOB_TYPE : jobType.getName();

    JobResult result = new JobResult();
    // Note: before adding Spark analysers, all JobResult are using job ids as the primary key. But Spark (and many
    // other non-mapreduce applications) does not have a job id. To maintain backwards compatibility, we replace
    // 'application' with 'job' to form a pseudo one.
    result.jobId = Utils.getJobIdFromApplicationId(getAppId());
    result.url = getTrackingUrl();
    result.username = getUser();
    result.startTime = getStartTime();
    result.analysisTime = System.currentTimeMillis();
    result.jobName = getName();
    result.jobType = jobTypeName;

    // Truncate long names
    if (result.jobName.length() > 100) {
      result.jobName = result.jobName.substring(0, 97) + "...";
    }
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
