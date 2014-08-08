package com.linkedin.drelephant;

import com.google.common.collect.Lists;
import com.linkedin.drelephant.hadoop.HadoopCounterHolder;
import com.linkedin.drelephant.hadoop.HadoopCounterHolder.CounterName;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;

import model.JobResult;

import org.apache.hadoop.conf.Configuration;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;


public class ElephantFetcherYarn implements ElephantFetcher {
  private static final Logger logger = Logger.getLogger(ElephantFetcher.class);

  private RetryFactory retryFactory;
  private URLFactory urlFactory;
  private JSONFactory jsonFactory;
  private boolean firstTime = true;
  private long lastTime = 0;
  private long currentTime = 0;

  public ElephantFetcherYarn(Configuration hadoopConf) throws IOException {
    init(hadoopConf);
  }

  private void init(Configuration hadoopConf) throws IOException {
    logger.info("Connecting to the job history server...");
    String jhistoryAddr = hadoopConf.get("mapreduce.jobhistory.webapp.address");
    urlFactory = new URLFactory(jhistoryAddr);
    jsonFactory = new JSONFactory();
    retryFactory = new RetryFactory();
    logger.info("Connection success.");
  }

  /*
   * Fetch job list to analyze
   * If first time, search time span from 0 to now, check database for each job
   * If not first time, search time span since last fetch, also re-fetch failed jobs
   * Return list on success, throw Exception on error
   */
  public List<HadoopJobData> fetchJobList() throws IOException, AuthenticationException {

    List<HadoopJobData> jobList;

    currentTime = System.currentTimeMillis();
    URL joblistURL = urlFactory.fetchJobListURL(lastTime, currentTime);

    jobList = jsonFactory.getJobData(joblistURL, firstTime);
    if (firstTime) {
      firstTime = false;
    } else {
      // If not first time, also fetch jobs that need to retry
      jobList.addAll(retryFactory.getJobs());
    }

    lastTime = currentTime;

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
      clearJobData(jobData);
      // Add to retry list
      retryFactory.addJob(jobData);
    }
  }

  // Fetch job detailed data. Return true on success
  public void fetchJobData(HadoopJobData jobData) throws IOException, AuthenticationException {
    String jobId = jobData.getJobId();

    // Fetch job counter
    URL jobCounterURL = urlFactory.getJobCounterURL(jobId);
    HadoopCounterHolder jobCounter = jsonFactory.getJobCounter(jobCounterURL);

    // Fetch job config
    URL jobConfigURL = urlFactory.getJobConfigURL(jobId);
    Properties jobConf = jsonFactory.getProperties(jobConfigURL);

    // Fetch task data
    URL taskListURL = urlFactory.getTaskListURL(jobId);
    List<HadoopTaskData> mapperList = new ArrayList<HadoopTaskData>();
    List<HadoopTaskData> reducerList = new ArrayList<HadoopTaskData>();
    jsonFactory.getTaskDataAll(taskListURL, jobId, mapperList, reducerList);

    HadoopTaskData[] mapperData = mapperList.toArray(new HadoopTaskData[mapperList.size()]);
    HadoopTaskData[] reducerData = reducerList.toArray(new HadoopTaskData[reducerList.size()]);

    jobData.setCounters(jobCounter).setMapperData(mapperData).setReducerData(reducerData).setJobConf(jobConf);
  }

  private String getJobDetailURL(String jobId) {
    return urlFactory.getJobDetailURLString(jobId);
  }

  private URL getTaskCounterURL(String jobId, String taskId) throws MalformedURLException {
    return urlFactory.getTaskCounterURL(jobId, taskId);
  }

  private URL getTaskAttemptURL(String jobId, String taskId, String attemptId) throws MalformedURLException {
    return urlFactory.getTaskAttemptURL(jobId, taskId, attemptId);
  }

  private class URLFactory {

    private String root;
    private String restRoot;

    public URLFactory(String hserverAddr) throws IOException {
      root = "http://" + hserverAddr;
      restRoot = "http://" + hserverAddr + "/ws/v1/history/mapreduce/jobs";
      verifyURL(restRoot);
    }

    private void verifyURL(String url) throws IOException {
      final URLConnection connection = new URL(url).openConnection();
      // Check service availability
      connection.connect();
      return;
    }

    private String getJobDetailURLString(String jobId) {
      return root + "/jobhistory/job/" + jobId;
    }

    private URL fetchJobListURL(long startTime, long endTime) throws MalformedURLException {
      return new URL(restRoot + "?finishedTimeBegin=" + startTime + "&finishedTimeEnd=" + endTime + "&state=SUCCEEDED");
    }

    private URL getJobConfigURL(String jobId) throws MalformedURLException {
      return new URL(restRoot + "/" + jobId + "/conf");
    }

    private URL getJobCounterURL(String jobId) throws MalformedURLException {
      return new URL(restRoot + "/" + jobId + "/counters");
    }

    private URL getTaskListURL(String jobId) throws MalformedURLException {
      return new URL(restRoot + "/" + jobId + "/tasks");
    }

    private URL getTaskCounterURL(String jobId, String taskId) throws MalformedURLException {
      return new URL(restRoot + "/" + jobId + "/tasks/" + taskId + "/counters");
    }

    private URL getTaskAttemptURL(String jobId, String taskId, String attemptId) throws MalformedURLException {
      return new URL(restRoot + "/" + jobId + "/tasks/" + taskId + "/attempts/" + attemptId);
    }
  }

  private class JSONFactory {
    private ObjectMapper mapper = new ObjectMapper();
    private AuthenticatedURL.Token token = new AuthenticatedURL.Token();
    private AuthenticatedURL authenticatedURL = new AuthenticatedURL();
    private Set<String> counterSet = new HashSet<String>();;

    public JSONFactory() {
      // Store the set of counters we want to fetch
      for (CounterName counter : CounterName.values()) {
        counterSet.add(counter.getName());
      }
    }

    private List<HadoopJobData> getJobData(URL url, boolean checkDB) throws IOException, AuthenticationException {
      List<HadoopJobData> jobList = new ArrayList<HadoopJobData>();

      HttpURLConnection conn = authenticatedURL.openConnection(url, token);
      JsonNode rootNode = mapper.readTree(conn.getInputStream());
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
            .setJobName(job.get("name").getValueAsText()).setUrl(getJobDetailURL(jobId));

        jobList.add(jobData);
      }
      return jobList;
    }

    private Properties getProperties(URL url) throws IOException, AuthenticationException {
      Properties jobConf = new Properties();

      HttpURLConnection conn = authenticatedURL.openConnection(url, token);
      JsonNode rootNode = mapper.readTree(conn.getInputStream());
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

      HttpURLConnection conn = authenticatedURL.openConnection(url, token);
      JsonNode rootNode = mapper.readTree(conn.getInputStream());
      JsonNode groups = rootNode.path("jobCounters").path("counterGroup");

      for (JsonNode group : groups) {
        for (JsonNode counter : group.path("counter")) {
          String name = counter.get("name").getValueAsText();
          if (counterSet.contains(name)) {
            // This is a counter we want to fetch
            long val = counter.get("totalCounterValue").getLongValue();
            counterMap.put(CounterName.valueOf(name), val);
          }
        }
      }
      // For every missing counters in the job, set with default value 0
      for (CounterName name : CounterName.values()) {
        if (!counterMap.containsKey(name)) {
          counterMap.put(name, 0L);
        }
      }
      return new HadoopCounterHolder(counterMap);
    }

    private HadoopCounterHolder getTaskCounter(URL url) throws IOException, AuthenticationException {
      Map<CounterName, Long> counterMap = new EnumMap<CounterName, Long>(CounterName.class);

      HttpURLConnection conn = authenticatedURL.openConnection(url, token);
      JsonNode rootNode = mapper.readTree(conn.getInputStream());
      JsonNode groups = rootNode.path("jobTaskCounters").path("taskCounterGroup");

      for (JsonNode group : groups) {
        for (JsonNode counter : group.path("counter")) {
          String name = counter.get("name").getValueAsText();
          if (counterSet.contains(name)) {
            long val = counter.get("value").getLongValue();
            counterMap.put(CounterName.valueOf(name), val);
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
      HttpURLConnection conn = authenticatedURL.openConnection(url, token);
      JsonNode rootNode = mapper.readTree(conn.getInputStream());
      JsonNode taskAttempt = rootNode.path("taskAttempt");

      long startTime = taskAttempt.get("startTime").getLongValue();
      long finishTime = taskAttempt.get("finishTime").getLongValue();
      boolean isMapper = taskAttempt.get("type").getValueAsText().equals("MAP");

      long[] time;
      if (isMapper) {
        // No shuffle sore time in Mapper
        time = new long[] { startTime, finishTime, 0, 0 };
      } else {
        long shuffleTime = taskAttempt.get("elapsedShuffleTime").getLongValue();
        long sortTime = taskAttempt.get("elapsedMergeTime").getLongValue();
        time = new long[] { startTime, finishTime, shuffleTime, sortTime };
      }

      return time;
    }

    private void getTaskDataAll(URL url, String jobId, List<HadoopTaskData> mapperList, List<HadoopTaskData> reducerList)
        throws IOException, AuthenticationException {
      HttpURLConnection conn = authenticatedURL.openConnection(url, token);
      JsonNode rootNode = mapper.readTree(conn.getInputStream());
      JsonNode tasks = rootNode.path("tasks").path("task");

      for (JsonNode task : tasks) {
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
    private static final int DEFAULT_RETRY = 3;
    private Map<HadoopJobData, Integer> retryMap = new HashMap<HadoopJobData, Integer>();

    private void addJob(HadoopJobData job) {
      if (retryMap.containsKey(job)) {
        // This is old retry job
        int retryLeft = retryMap.get(job);
        if (retryLeft == 1) {
          // Drop job on max retries
          logger.error("Drop job. Reason: reach max retry for job id=" + job.getJobId());
          retryMap.remove(job);
        } else {
          retryMap.put(job, retryLeft - 1);
        }
      } else {
        // This is new retry job
        retryMap.put(job, DEFAULT_RETRY);
      }
    }

    private List<HadoopJobData> getJobs() {
      return Lists.newArrayList(retryMap.keySet());
    }
  }

}
