package com.linkedin.drelephant;

import com.google.common.collect.Lists;
import com.linkedin.drelephant.hadoop.HadoopCounterHolder;
import com.linkedin.drelephant.hadoop.HadoopCounterHolder.CounterName;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;
import com.linkedin.drelephant.math.Statistics;

import model.JobResult;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;


public class ElephantFetcherYarn implements ElephantFetcher {
  private static final Logger logger = Logger.getLogger(ElephantFetcher.class);

  private RetryFactory _retryFactory;
  private URLFactory _urlFactory;
  private JSONFactory _jsonFactory;
  private boolean _firstRun = true;
  private long _lastTime = 0;
  private long _currentTime = 0;

  public ElephantFetcherYarn(JobConf hadoopConf) throws IOException {
    logger.info("Connecting to the job history server...");
    String jhistoryAddr = hadoopConf.get("mapreduce.jobhistory.webapp.address");
    _urlFactory = new URLFactory(jhistoryAddr);
    _jsonFactory = new JSONFactory();
    _retryFactory = new RetryFactory();
    logger.info("Connection success.");
  }

  public void init(int threadId) throws IOException {
    ThreadContextMR2.init(threadId);
  }

  /*
   * Fetch job list to analyze
   * If first time, search time span from 0 to now, check database for each job
   * If not first time, search time span since last fetch, also re-fetch failed jobs
   * Return list on success, throw Exception on error
   */
  public List<HadoopJobData> fetchJobList() throws IOException, AuthenticationException {

    List<HadoopJobData> jobList;
    int retrySize = 0;

    _currentTime = System.currentTimeMillis();
    URL joblistURL= _urlFactory.fetchJobListURL(_lastTime, _currentTime);

    try {
      jobList = _jsonFactory.getJobData(joblistURL, _firstRun);
    } finally {
      ThreadContextMR2.updateAuthToken();
    }

    if (_firstRun) {
      _firstRun = false;
    } else {
      List<HadoopJobData> retryList = _retryFactory.fetchAllRetryJobs();
      // If not first time, also fetch jobs that need to retry
      retrySize = retryList.size();
      jobList.addAll(retryList);
    }

    _lastTime = _currentTime;

    logger.info("Fetcher gets total " + jobList.size() + " jobs (" + retrySize + " retry jobs)");

    return jobList;
  }

  // Check database to see if a job is already analyzed
  private boolean checkDBforJob(String jobId) {
    JobResult result = JobResult.find.byId(jobId);
    return (result != null);
  }

  // Clear all data stored on the job object
  private void clearJobData(HadoopJobData jobData) {
    jobData.setCounters(null).setJobConf(null).setMapperData(null).setReducerData(null);
  }

  // OnJobFinish Add to retry list upon failure
  public void finishJob(HadoopJobData jobData, boolean success) {
    if (!success) {
      if (!jobData.isRetryJob()) {
        jobData.setRetry(true);
      }
      clearJobData(jobData);
      // Add to retry list
      _retryFactory.addJobToRetryList(jobData);
    } else {
      if (jobData.isRetryJob()) {
        // If it is retry job, remove it from retry map
        _retryFactory.checkAndRemoveFromRetryList(jobData);
      }
    }
  }

  // Fetch job detailed data. Return true on success
  public void fetchJobData(HadoopJobData jobData) throws IOException, AuthenticationException {
    try {
      String jobId = jobData.getJobId();

      // Fetch job counter
      URL jobCounterURL = _urlFactory.getJobCounterURL(jobId);
      HadoopCounterHolder jobCounter = _jsonFactory.getJobCounter(jobCounterURL);

      // Fetch job config
      URL jobConfigURL = _urlFactory.getJobConfigURL(jobId);
      Properties jobConf = _jsonFactory.getProperties(jobConfigURL);

      // Fetch task data
      URL taskListURL = _urlFactory.getTaskListURL(jobId);
      List<HadoopTaskData> mapperList = new ArrayList<HadoopTaskData>();
      List<HadoopTaskData> reducerList = new ArrayList<HadoopTaskData>();
      _jsonFactory.getTaskDataAll(taskListURL, jobId, mapperList, reducerList);

      HadoopTaskData[] mapperData = mapperList.toArray(new HadoopTaskData[mapperList.size()]);
      HadoopTaskData[] reducerData = reducerList.toArray(new HadoopTaskData[reducerList.size()]);

      jobData.setCounters(jobCounter).setMapperData(mapperData).setReducerData(reducerData).setJobConf(jobConf);

    } finally {
      ThreadContextMR2.updateAuthToken();
    }
  }

  private String getJobDetailURL(String jobId) {
    return _urlFactory.getJobDetailURLString(jobId);
  }

  private URL getTaskCounterURL(String jobId, String taskId) throws MalformedURLException {
    return _urlFactory.getTaskCounterURL(jobId, taskId);
  }

  private URL getTaskAttemptURL(String jobId, String taskId, String attemptId) throws MalformedURLException {
    return _urlFactory.getTaskAttemptURL(jobId, taskId, attemptId);
  }

  private class URLFactory {

    private String _root;
    private String _restRoot;

    public URLFactory(String hserverAddr) throws IOException {
      _root = "http://" + hserverAddr;
      _restRoot = "http://" + hserverAddr + "/ws/v1/history/mapreduce/jobs";
      verifyURL(_restRoot);
    }

    private void verifyURL(String url) throws IOException {
      final URLConnection connection = new URL(url).openConnection();
      // Check service availability
      connection.connect();
      return;
    }

    private String getJobDetailURLString(String jobId) {
      return _root + "/jobhistory/job/" + jobId;
    }

    private URL fetchJobListURL(long startTime, long endTime) throws MalformedURLException {
      return new URL(_restRoot + "?finishedTimeBegin=" + startTime + "&finishedTimeEnd=" + endTime + "&state=SUCCEEDED");
    }

    private URL getJobConfigURL(String jobId) throws MalformedURLException {
      return new URL(_restRoot + "/" + jobId + "/conf");
    }

    private URL getJobCounterURL(String jobId) throws MalformedURLException {
      return new URL(_restRoot + "/" + jobId + "/counters");
    }

    private URL getTaskListURL(String jobId) throws MalformedURLException {
      return new URL(_restRoot + "/" + jobId + "/tasks");
    }

    private URL getTaskCounterURL(String jobId, String taskId) throws MalformedURLException {
      return new URL(_restRoot + "/" + jobId + "/tasks/" + taskId + "/counters");
    }

    private URL getTaskAttemptURL(String jobId, String taskId, String attemptId) throws MalformedURLException {
      return new URL(_restRoot + "/" + jobId + "/tasks/" + taskId + "/attempts/" + attemptId);
    }
  }

  private class JSONFactory {
    private List<HadoopJobData> getJobData(URL url, boolean checkDB) throws IOException, AuthenticationException {
      List<HadoopJobData> jobList = new ArrayList<HadoopJobData>();

      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      JsonNode jobs = rootNode.path("jobs").path("job");

      for (JsonNode job : jobs) {
        String jobId = job.get("id").getValueAsText();

        // On first time, for every job, we check database
        if (checkDB && checkDBforJob(jobId)) {
          continue;
        }

        // New job
        HadoopJobData jobData = new HadoopJobData();
        jobData.setJobId(jobId).setUsername(job.get("user").getValueAsText())
            .setJobName(job.get("name").getValueAsText()).setUrl(getJobDetailURL(jobId))
            .setStartTime(job.get("startTime").getLongValue());

        jobList.add(jobData);
      }
      return jobList;
    }

    private Properties getProperties(URL url) throws IOException, AuthenticationException {
      Properties jobConf = new Properties();

      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      JsonNode configs = rootNode.path("conf").path("property");

      for (JsonNode conf : configs) {
        String key = conf.get("name").getValueAsText();
        String val = conf.get("value").getValueAsText();
        jobConf.setProperty(key, val);
      }
      return jobConf;
    }

    private HadoopCounterHolder getJobCounter(URL url) throws IOException, AuthenticationException {
      Map<CounterName, Long> counterMap = new EnumMap<CounterName, Long>(CounterName.class);

      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      JsonNode groups = rootNode.path("jobCounters").path("counterGroup");

      for (JsonNode group : groups) {
        for (JsonNode counter : group.path("counter")) {
          String name = counter.get("name").getValueAsText();
          CounterName cn = CounterName.getCounterFromName(name);
          if (cn != null) {
            counterMap.put(cn, counter.get("totalCounterValue").getLongValue());
          }
        }
      }
      return new HadoopCounterHolder(counterMap);
    }

    private HadoopCounterHolder getTaskCounter(URL url) throws IOException, AuthenticationException {
      Map<CounterName, Long> counterMap = new EnumMap<CounterName, Long>(CounterName.class);

      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      JsonNode groups = rootNode.path("jobTaskCounters").path("taskCounterGroup");

      for (JsonNode group : groups) {
        for (JsonNode counter : group.path("counter")) {
          String name = counter.get("name").getValueAsText();
          CounterName cn = CounterName.getCounterFromName(name);
          if (cn != null) {
            counterMap.put(cn, counter.get("value").getLongValue());
          }
        }
      }

      for (CounterName name : CounterName.values()) {
        if (!counterMap.containsKey(name)) {
          counterMap.put(name, 0L);
        }
      }
      return new HadoopCounterHolder(counterMap);
    }

    private long[] getTaskExecTime(URL url) throws IOException, AuthenticationException {

      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      JsonNode taskAttempt = rootNode.path("taskAttempt");

      long startTime = taskAttempt.get("startTime").getLongValue();
      long finishTime = taskAttempt.get("finishTime").getLongValue();
      boolean isMapper = taskAttempt.get("type").getValueAsText().equals("MAP");

      long[] time;
      if (isMapper) {
        // No shuffle sore time in Mapper
        time = new long[] { finishTime - startTime, 0, 0 };
      } else {
        long shuffleTime = taskAttempt.get("elapsedShuffleTime").getLongValue();
        long sortTime = taskAttempt.get("elapsedMergeTime").getLongValue();
        time = new long[] { finishTime - startTime, shuffleTime, sortTime };
      }

      return time;
    }

    private void getTaskDataAll(URL url, String jobId, List<HadoopTaskData> mapperList, List<HadoopTaskData> reducerList)
        throws IOException, AuthenticationException {

      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      JsonNode tasks = rootNode.path("tasks").path("task");

      for (JsonNode task : tasks) {
        String state = task.get("state").getValueAsText();
        if (!state.equals("SUCCEEDED")) {
          // This is a failed task.
          continue;
        }
        String taskId = task.get("id").getValueAsText();
        String attemptId = task.get("successfulAttempt").getValueAsText();
        boolean isMapper = task.get("type").getValueAsText().equals("MAP");

        URL taskCounterURL = getTaskCounterURL(jobId, taskId);
        HadoopCounterHolder taskCounter = getTaskCounter(taskCounterURL);

        URL taskAttemptURL = getTaskAttemptURL(jobId, taskId, attemptId);
        long[] taskExecTime = getTaskExecTime(taskAttemptURL);

        HadoopTaskData taskData = new HadoopTaskData(taskCounter, taskExecTime);
        if (isMapper) {
          mapperList.add(taskData);
        } else {
          reducerList.add(taskData);
        }
      }
    }
  }

  private class RetryFactory {
    private static final int DEFAULT_RETRY = 2;
    private Map<HadoopJobData, Integer> _retryMapInWait = new ConcurrentHashMap<HadoopJobData, Integer>();
    private Map<HadoopJobData, Integer> _retryMapInProgress = new ConcurrentHashMap<HadoopJobData, Integer>();

    private void addJobToRetryList(HadoopJobData job) {
      if (_retryMapInProgress.containsKey(job)) {
        // This is old retry job
        int retryLeft = _retryMapInProgress.get(job);
        _retryMapInProgress.remove(job);
        if (retryLeft == 0) {
          // Drop job on max retries
          logger.error("Drop job. Reason: reach max retry for job id=" + job.getJobId());
        } else {
          _retryMapInWait.put(job, retryLeft);
        }
      } else {
        // This is new retry job
        _retryMapInWait.put(job, DEFAULT_RETRY);
      }
    }

    private List<HadoopJobData> fetchAllRetryJobs() {
      List<HadoopJobData> retryList = new ArrayList<HadoopJobData>();
      for (HadoopJobData job : _retryMapInWait.keySet()) {
        int retry = _retryMapInWait.get(job);
        _retryMapInWait.remove(job);
        retryList.add(job);
        _retryMapInProgress.put(job, retry - 1);
      }
      return Lists.newArrayList(retryList);
    }

    private void checkAndRemoveFromRetryList(HadoopJobData jobData) {
      if (_retryMapInProgress.containsKey(jobData)) {
        _retryMapInProgress.remove(jobData);
      }
    }
  }
}

final class ThreadContextMR2 {
  private static final Logger logger = Logger.getLogger(ThreadContextMR2.class);
  private static final ThreadLocal<Integer> _LOCAL_THREAD_ID = new ThreadLocal<Integer>();
  private static final ThreadLocal<AuthenticatedURL.Token> _LOCAL_AUTH_TOKEN =
      new ThreadLocal<AuthenticatedURL.Token>();
  private static final ThreadLocal<AuthenticatedURL> _LOCAL_AUTH_URL = new ThreadLocal<AuthenticatedURL>();
  private static final ThreadLocal<ObjectMapper> _LOCAL_MAPPER = new ThreadLocal<ObjectMapper>();
  private static final ThreadLocal<Long> _LOCAL_LAST_UPDATED = new ThreadLocal<Long>();
  private static final ThreadLocal<Long> _LOCAL_UPDATE_INTERVAL = new ThreadLocal<Long>();

  private ThreadContextMR2() {
  }

  static void init(int threadId) throws IOException {
    _LOCAL_AUTH_TOKEN.set(new AuthenticatedURL.Token());
    _LOCAL_AUTH_URL.set(new AuthenticatedURL());
    _LOCAL_MAPPER.set(new ObjectMapper());
    _LOCAL_THREAD_ID.set(threadId);
    _LOCAL_LAST_UPDATED.set(System.currentTimeMillis());
    // Random an interval for each executor to avoid update token at the same time
    _LOCAL_UPDATE_INTERVAL.set(Statistics.MINUTE_IN_MS * 30 + new Random().nextLong() % (3 * Statistics.MINUTE_IN_MS));
    logger.info("Executor " + _LOCAL_THREAD_ID.get() + " update interval " + _LOCAL_UPDATE_INTERVAL.get() * 1.0
        / Statistics.MINUTE_IN_MS);
  }

  public static JsonNode readJsonNode(URL url) throws IOException, AuthenticationException {
    HttpURLConnection conn = _LOCAL_AUTH_URL.get().openConnection(url, _LOCAL_AUTH_TOKEN.get());
    return _LOCAL_MAPPER.get().readTree(conn.getInputStream());
  }

  public static void updateAuthToken() {
    long curTime = System.currentTimeMillis();
    if (curTime - _LOCAL_LAST_UPDATED.get() > _LOCAL_UPDATE_INTERVAL.get()) {
      logger.info("Executor " + _LOCAL_THREAD_ID.get() + " updates its AuthenticatedToken.");
      _LOCAL_AUTH_TOKEN.set(new AuthenticatedURL.Token());
      _LOCAL_AUTH_URL.set(new AuthenticatedURL());
      _LOCAL_LAST_UPDATED.set(curTime);
    }
  }
}
