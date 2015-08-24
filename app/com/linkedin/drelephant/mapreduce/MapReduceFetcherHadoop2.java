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
package com.linkedin.drelephant.mapreduce;

import com.google.common.collect.Lists;
import com.linkedin.drelephant.analysis.ElephantFetcher;
import com.linkedin.drelephant.mapreduce.MapReduceCounterHolder.CounterName;
import com.linkedin.drelephant.math.Statistics;
import com.linkedin.drelephant.util.Utils;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import model.JobResult;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;


public class MapReduceFetcherHadoop2 implements ElephantFetcher<MapReduceApplicationData> {
  private static final Logger logger = Logger.getLogger(ElephantFetcher.class);
  // We provide one minute job fetch delay due to the job sending lag from AM/NM to JobHistoryServer HDFS
  private static final long FETCH_DELAY = 60000;

  private RetryFactory _retryFactory;
  private URLFactory _urlFactory;
  private JSONFactory _jsonFactory;
  private boolean _firstRun = true;
  private long _lastTime = 0;
  private long _currentTime = 0;

  public MapReduceFetcherHadoop2() throws IOException {
    logger.info("Connecting to the job history server...");
    final String jhistoryAddr = new JobConf().get("mapreduce.jobhistory.webapp.address");
    _urlFactory = new URLFactory(jhistoryAddr);
    _jsonFactory = new JSONFactory();
    _retryFactory = new RetryFactory();
    logger.info("Connection success.");
  }

  /*
   * Fetch job list to analyze
   * If first time, search time span from 0 to now, check database for each job
   * If not first time, search time span since last fetch, also re-fetch failed jobs
   * Return list on success, throw Exception on error
   */
  public List<MapReduceApplicationData> fetchJobList() throws IOException, AuthenticationException {

    List<MapReduceApplicationData> jobList;
    int retrySize = 0;

    // There is a lag of job data from AM/NM to JobHistoryServer HDFS, we shouldn't use the current
    // time, since there might be new jobs arriving after we fetch jobs.
    // We provide one minute delay to address this lag.
    _currentTime = System.currentTimeMillis() - FETCH_DELAY;
    URL joblistURL= _urlFactory.fetchJobListURL(_lastTime, _currentTime);

    try {
      jobList = _jsonFactory.getJobData(joblistURL, _firstRun);
    } finally {
      ThreadContextMR2.updateAuthToken();
    }

    if (_firstRun) {
      _firstRun = false;
    } else {
      List<MapReduceApplicationData> retryList = _retryFactory.fetchAllRetryJobs();
      // If not first time, also fetch jobs that need to retry
      retrySize = retryList.size();
      jobList.addAll(retryList);
    }

    logger.info("Fetcher gets total " + jobList.size() + " jobs (" + retrySize + " retry jobs) from time range ["
        + _lastTime + "," + _currentTime + "]");
    _lastTime = _currentTime;

    return jobList;
  }

  // Check database to see if a job is already analyzed
  private boolean checkDBforJob(String jobId) {
    JobResult result = JobResult.find.byId(jobId);
    return (result != null);
  }

  // Clear all data stored on the job object
  private void clearJobData(MapReduceApplicationData jobData) {
    jobData.setCounters(null).setJobConf(null).setMapperData(null).setReducerData(null);
  }

  // OnJobFinish Add to retry list upon failure
  public void finishJob(MapReduceApplicationData jobData, boolean success) {
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
  @Override
  public MapReduceApplicationData fetchData(String appId) throws IOException, AuthenticationException {
    MapReduceApplicationData jobData = new MapReduceApplicationData();
    String jobId = Utils.getJobIdFromApplicationId(appId);
    jobData.setAppId(appId).setJobId(jobId);
    try {
      // Fetch job counter
      URL jobCounterURL = _urlFactory.getJobCounterURL(jobId);
      MapReduceCounterHolder jobCounter = _jsonFactory.getJobCounter(jobCounterURL);

      // Fetch job config
      URL jobConfigURL = _urlFactory.getJobConfigURL(jobId);
      Properties jobConf = _jsonFactory.getProperties(jobConfigURL);

      // Fetch task data
      URL taskListURL = _urlFactory.getTaskListURL(jobId);
      List<MapReduceTaskData> mapperList = new ArrayList<MapReduceTaskData>();
      List<MapReduceTaskData> reducerList = new ArrayList<MapReduceTaskData>();
      _jsonFactory.getTaskDataAll(taskListURL, jobId, mapperList, reducerList);

      MapReduceTaskData[] mapperData = mapperList.toArray(new MapReduceTaskData[mapperList.size()]);
      MapReduceTaskData[] reducerData = reducerList.toArray(new MapReduceTaskData[reducerList.size()]);

      jobData.setCounters(jobCounter).setMapperData(mapperData).setReducerData(reducerData).setJobConf(jobConf);

    } finally {
      ThreadContextMR2.updateAuthToken();
    }

    Utils.publishMetrics(jobData);

    return jobData;
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

    private URLFactory(String hserverAddr) throws IOException {
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

    public URL fetchJobListURL(long startTime, long endTime) throws MalformedURLException {
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
    private List<MapReduceApplicationData> getJobData(URL url, boolean checkDB) throws IOException, AuthenticationException {
      List<MapReduceApplicationData> jobList = new ArrayList<MapReduceApplicationData>();

      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      JsonNode jobs = rootNode.path("jobs").path("job");

      for (JsonNode job : jobs) {
        String jobId = job.get("id").getValueAsText();

        // On first time, for every job, we check database
        if (checkDB && checkDBforJob(jobId)) {
          continue;
        }

        // New job
        /*
           {
           "finishTime": 1415816989533,
           "id": "job_1415656299984_0014",
           "mapsCompleted": 1,
           "mapsTotal": 1,
           "name": "word count",
           "queue": "default",
           "reducesCompleted": 1,
           "reducesTotal": 1,
           "startTime": 1415816979666,
           "state": "SUCCEEDED",
           "submitTime": 1415816976300,
           "user": "ssubrama"
           }
         */
        MapReduceApplicationData jobData = new MapReduceApplicationData();
        jobData.setJobId(jobId).setUsername(job.get("user").getValueAsText())
            .setJobName(job.get("name").getValueAsText()).setUrl(getJobDetailURL(jobId))
            .setStartTime(job.get("startTime").getLongValue())
            .setFinishTime(job.get("finishTime").getLongValue());

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

    private MapReduceCounterHolder getJobCounter(URL url)
        throws IOException, AuthenticationException {
      MapReduceCounterHolder holder = new MapReduceCounterHolder();

      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      JsonNode groups = rootNode.path("jobCounters").path("counterGroup");

      for (JsonNode group : groups) {
        for (JsonNode counter : group.path("counter")) {
          String counterName = counter.get("name").getValueAsText();
          CounterName cn = CounterName.getCounterFromName(counterName);
          Long counterValue = counter.get("totalCounterValue").getLongValue();
          String groupName = group.get("counterGroupName").getValueAsText();
          holder.set(groupName, counterName, counterValue);
        }
      }
      return holder;
    }

    private MapReduceCounterHolder getTaskCounter(URL url) throws IOException, AuthenticationException {
      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      JsonNode groups = rootNode.path("jobTaskCounters").path("taskCounterGroup");
      MapReduceCounterHolder holder = new MapReduceCounterHolder();

      for (JsonNode group : groups) {
        for (JsonNode counter : group.path("counter")) {
          String name = counter.get("name").getValueAsText();
          String groupName = group.get("counterGroupName").getValueAsText();
          Long value = counter.get("value").getLongValue();
          holder.set(groupName, name, value);
        }
      }

      return holder;
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

    private void getTaskDataAll(URL url, String jobId, List<MapReduceTaskData> mapperList, List<MapReduceTaskData> reducerList)
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
        MapReduceCounterHolder taskCounter = getTaskCounter(taskCounterURL);

        URL taskAttemptURL = getTaskAttemptURL(jobId, taskId, attemptId);
        long[] taskExecTime = getTaskExecTime(taskAttemptURL);

        MapReduceTaskData taskData = new MapReduceTaskData(taskCounter, taskExecTime);
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
    private Map<MapReduceApplicationData, Integer> _retryMapInWait = new ConcurrentHashMap<MapReduceApplicationData, Integer>();
    private Map<MapReduceApplicationData, Integer> _retryMapInProgress = new ConcurrentHashMap<MapReduceApplicationData, Integer>();

    private void addJobToRetryList(MapReduceApplicationData job) {
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

    private List<MapReduceApplicationData> fetchAllRetryJobs() {
      List<MapReduceApplicationData> retryList = new ArrayList<MapReduceApplicationData>();
      for (MapReduceApplicationData job : _retryMapInWait.keySet()) {
        int retry = _retryMapInWait.get(job);
        _retryMapInWait.remove(job);
        retryList.add(job);
        _retryMapInProgress.put(job, retry - 1);
      }
      return Lists.newArrayList(retryList);
    }

    private void checkAndRemoveFromRetryList(MapReduceApplicationData jobData) {
      if (_retryMapInProgress.containsKey(jobData)) {
        _retryMapInProgress.remove(jobData);
      }
    }
  }
}

final class ThreadContextMR2 {
  private static final Logger logger = Logger.getLogger(ThreadContextMR2.class);

  private static final AtomicInteger THREAD_ID = new AtomicInteger(1);

  private static final ThreadLocal<Integer> _LOCAL_THREAD_ID = new ThreadLocal<Integer>() {
    @Override
    public Integer initialValue() {
      return THREAD_ID.getAndIncrement();
    }
  };

  private static final ThreadLocal<Long> _LOCAL_LAST_UPDATED = new ThreadLocal<Long>();
  private static final ThreadLocal<Long> _LOCAL_UPDATE_INTERVAL = new ThreadLocal<Long>();

  private static final ThreadLocal<AuthenticatedURL.Token> _LOCAL_AUTH_TOKEN =
      new ThreadLocal<AuthenticatedURL.Token>() {
    @Override
    public AuthenticatedURL.Token initialValue() {
      _LOCAL_LAST_UPDATED.set(System.currentTimeMillis());
      // Random an interval for each executor to avoid update token at the same time
      _LOCAL_UPDATE_INTERVAL.set(Statistics.MINUTE_IN_MS * 30 + new Random().nextLong() % (3 * Statistics.MINUTE_IN_MS));
      logger.info("Executor " + _LOCAL_THREAD_ID.get() + " update interval " + _LOCAL_UPDATE_INTERVAL.get() * 1.0
          / Statistics.MINUTE_IN_MS);
      return new AuthenticatedURL.Token();
    }
  };

  private static final ThreadLocal<AuthenticatedURL> _LOCAL_AUTH_URL = new ThreadLocal<AuthenticatedURL>() {
    @Override
    public AuthenticatedURL initialValue() {
      return new AuthenticatedURL();
    }
  };

  private static final ThreadLocal<ObjectMapper> _LOCAL_MAPPER = new ThreadLocal<ObjectMapper>() {
    @Override
    public ObjectMapper initialValue() {
      return new ObjectMapper();
    }
  };

  private ThreadContextMR2() {
    // Empty on purpose
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
