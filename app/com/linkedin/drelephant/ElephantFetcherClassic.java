package com.linkedin.drelephant;

import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.hadoop.HadoopCounterHolder;
import com.linkedin.drelephant.hadoop.HadoopCounterHolder.CounterName;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;
import com.linkedin.drelephant.math.Statistics;

import model.JobResult;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ElephantFetcherClassic implements ElephantFetcher {
  private static final Logger logger = Logger.getLogger(ElephantFetcher.class);
  private static final int DEFAULT_RETRY = 2;
  private Configuration _conf;
  private Set<String> _previousJobs;
  private Map<HadoopJobData, Integer> _failedJobsInWait = new ConcurrentHashMap<HadoopJobData, Integer>();
  private Map<HadoopJobData, Integer> _failedJobsInProgress = new ConcurrentHashMap<HadoopJobData, Integer>();
  private boolean _firstRun = true;
  private String _jobtrackerHttpRoot;

  public ElephantFetcherClassic(Configuration hadoopConf) throws IOException {
    _conf = hadoopConf;
    _jobtrackerHttpRoot = "http://" + hadoopConf.get("mapred.job.tracker.http.address") + "/";
  }

  public void init(int threadId) throws IOException {
    ThreadContextMR1.init(_conf, threadId);
  }

  public List<HadoopJobData> fetchJobList() throws IOException {
    JobStatus[] result = null;

    JobClient jobClient = ThreadContextMR1.getClient();
    result = jobClient.getAllJobs();

    if (result == null) {
      throw new IOException("Error fetching joblist from jobtracker.");
    }

    // Get all completed jobs from jobtracker
    Set<String> successJobs = filterSuccessfulJobs(result);

    // Filter out newly completed jobs
    successJobs = filterPreviousJobs(successJobs);

    // Add newly completed jobs to return list
    List<HadoopJobData> jobList = new ArrayList<HadoopJobData>();
    for (String jobId : successJobs) {
      jobList.add(new HadoopJobData().setJobId(jobId));
    }
    // Add previously failed jobs to return list
    List<HadoopJobData> retryList = fetchAllFailedJobs();
    jobList.addAll(retryList);
    logger.info("Fetcher gets total " + jobList.size() + " jobs (" + retryList.size() + " retry jobs)");
    return jobList;
  }

  public void finishJob(HadoopJobData jobData, boolean success) {
    if (success) {
      // Job succeeded, remove from retry list if this is a retry job
      if(jobData.isRetryJob()) {
        checkAndRemoveJobFromRetryList(jobData);
      }
    } else {
      // Job failed, clear job data and add to retry list
      if(!jobData.isRetryJob()) {
        jobData.setRetry(true);
      }
      clearJobData(jobData);
      addJobToRetryList(jobData);
    }
  }

  public void fetchJobData(HadoopJobData jobData) throws IOException, AuthenticationException {

    JobID jobId = JobID.forName(jobData.getJobId());
    JobClient jobClient = ThreadContextMR1.getClient();
    TaskReport[] mapperTasks = null;
    TaskReport[] reducerTasks = null;
    RunningJob job = jobClient.getJob(jobId);

    if (job != null) {
      mapperTasks = jobClient.getMapTaskReports(jobId);
      reducerTasks = jobClient.getReduceTaskReports(jobId);
    }

    if (job == null || (mapperTasks.length == 0 && reducerTasks.length == 0)) {
      boolean retired = checkRetiredAndFetchJobData(jobData);
      if (retired) {
        logger.info(jobData.getJobId() + " retired. Fetch data from web UI.");
        ThreadContextMR1.updateAuthToken();
        return;
      }
    }

    JobStatus status = job.getJobStatus();
    String username = status.getUsername();
    long startTime = status.getStartTime();
    String jobUrl = job.getTrackingURL();
    String jobName = job.getJobName();
    jobData.setUsername(username).setStartTime(startTime).setUrl(jobUrl).setJobName(jobName);

    // Fetch job counter
    HadoopCounterHolder counterHolder = fetchCounter(job.getCounters());

    int sampleSize = Constants.SHUFFLE_SORT_MAX_SAMPLE_SIZE;

    // Fetch mapper task data
    List<HadoopTaskData> mapperList = new ArrayList<HadoopTaskData>();
    Statistics.shuffleArraySample(mapperTasks, sampleSize);
    for (int i = 0; i < mapperTasks.length; i++) {
      HadoopTaskData mapper = fetchTaskData(jobUrl, mapperTasks[i], true, (i < sampleSize));
      if (mapper != null) {
        mapperList.add(mapper);
      }
    }
    HadoopTaskData[] mappers = mapperList.toArray(new HadoopTaskData[mapperList.size()]);

    // Fetch reducer task data
    List<HadoopTaskData> reducerList = new ArrayList<HadoopTaskData>();
    Statistics.shuffleArraySample(reducerTasks, sampleSize);
    for (int i = 0; i < reducerTasks.length; i++) {
      HadoopTaskData reducer = fetchTaskData(jobUrl, reducerTasks[i], false, (i < sampleSize));
      if (reducer != null) {
        reducerList.add(reducer);
      }
    }
    HadoopTaskData[] reducers = reducerList.toArray(new HadoopTaskData[reducerList.size()]);

    // Fetch job config
    Properties jobConf = fetchJobConf(job);
    jobData.setCounters(counterHolder).setMapperData(mappers).setReducerData(reducers).setJobConf(jobConf);
    ThreadContextMR1.updateAuthToken();
  }

  private Properties fetchJobConf(RunningJob job) throws IOException, AuthenticationException {
    String jobconfUrl = getJobconfUrl(job);
    if (jobconfUrl == null) {
      return new Properties();
    }
    return fetchJobConfFromURL(jobconfUrl);
  }

  private Properties fetchJobConfFromURL(String jobconfUrl) throws IOException, AuthenticationException {

    //Parse job config from config page
    Properties properties = new Properties();
    Document doc = ThreadContextMR1.fetchHtmlDoc(jobconfUrl);
    Elements rows = doc.select("table").select("tr");
    for (Element row : rows) {
      Elements cells = row.select("> td");
      if (cells.size() == 2) {
        String key = cells.get(0).text().trim();
        String value = cells.get(1).text().trim();
        properties.put(key, value);
      }
    }
    return properties;
  }

  // Return a set of succeeded job ids
  private HashSet<String> filterSuccessfulJobs(JobStatus[] jobs) {
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
      return newJobs;
    } else {
      @SuppressWarnings("unchecked")
      Set<String> tempPrevJobs = (HashSet<String>) ((HashSet<String>) jobs).clone();
      // Leave only the newly completed jobs
      jobs.removeAll(_previousJobs);
      _previousJobs = tempPrevJobs;
      return jobs;
    }
  }

  private HadoopTaskData fetchTaskData(String jobDetailUrl, TaskReport task, boolean isMapper, boolean sampled)
      throws IOException, AuthenticationException {

    if (task.getCurrentStatus() != TIPStatus.COMPLETE) {
      // this is a failed task. Discard it.
      return null;
    }

    HadoopCounterHolder taskCounter = fetchCounter(task.getCounters());

    // This is not a sampled task, return task data with only counters
    if (!sampled) {
      return new HadoopTaskData(taskCounter);
    }

    // This is a sampled task, fetch task time from task detail page
    String taskDetailsUrl = getTaskDetailsPageUrl(jobDetailUrl, task.getTaskID().toString());
    long[] time = fetchTaskDetails(taskDetailsUrl, isMapper);
    return new HadoopTaskData(taskCounter, time);
  }

  private String getJobconfUrl(RunningJob job) {
    String jobDetails = job.getTrackingURL();
    String root = jobDetails.substring(0, jobDetails.indexOf("jobdetails.jsp"));
    return root + "jobconf.jsp?jobid=" + job.getID().toString();
  }

  private String getTaskDetailsPageUrl(String jobDetails, String taskId) {
    String root = jobDetails.substring(0, jobDetails.indexOf("jobdetails.jsp"));
    return root + "taskdetails.jsp?tipid=" + taskId.toString();
  }

  // Fetch task time from task detail page
  private long[] fetchTaskDetails(String taskDetailUrl, boolean isMapper) throws IOException, AuthenticationException {
    Document doc = ThreadContextMR1.fetchHtmlDoc(taskDetailUrl);
    Elements rows = doc.select("table").select("tr");
    long[] time = null;
    for (Element row : rows) {
      try {
        time = tryExtractDetailFromRow(row, isMapper);
        if (time != null) {
          return time;
        }
      } catch (Exception e) {
        throw new IOException("Error in fetch task data from task detail page. TASK URL=" + taskDetailUrl, e);
      }
    }
    throw new IOException("No valid time data found from task detail page. TASK URL=" + taskDetailUrl);
  }

  //Return shuffle sort time if successfully extracted data from table row (<tr>)
  private long[] tryExtractDetailFromRow(Element row, boolean isMapper) throws ParseException {
    Elements cells = row.select("> td");

    // For rows(<tr></tr>) in reducer task page with other than 12 cols(<td></td>),or 10 cols in mapper page,
    // they are not rows that contains time data
    if ((!isMapper && cells.size() != 12) || (isMapper && cells.size() != 10)) {
      return null;
    }

    boolean succeeded = cells.get(2).html().trim().equals("SUCCEEDED");
    if (succeeded) {
      SimpleDateFormat dateFormat = ThreadContextMR1.getDateFormat();
      if (!isMapper) {
        // reducer task. Get start finish shuffle sort time
        String startTime = cells.get(4).html().trim();
        String shuffleTime = cells.get(5).html().trim();
        String sortTime = cells.get(6).html().trim();
        String finishTime = cells.get(7).html().trim();
        if (shuffleTime.contains("(")) {
          shuffleTime = shuffleTime.substring(0, shuffleTime.indexOf("(") - 1);
        }
        if (sortTime.contains("(")) {
          sortTime = sortTime.substring(0, sortTime.indexOf("(") - 1);
        }
        if (finishTime.contains("(")) {
          finishTime = finishTime.substring(0, finishTime.indexOf("(") - 1);
        }
        long start = dateFormat.parse(startTime).getTime();
        long shuffle = dateFormat.parse(shuffleTime).getTime();
        long sort = dateFormat.parse(sortTime).getTime();
        long finish = dateFormat.parse(finishTime).getTime();

        long shuffleDuration = (shuffle - start);
        long sortDuration = (sort - shuffle);
        return new long[] { start, finish, shuffleDuration, sortDuration };
      } else {
        // Mapper task. Get start finish time
        String startTime = cells.get(4).html().trim();
        String finishTime = cells.get(5).html().trim();
        if (finishTime.contains("(")) {
          finishTime = finishTime.substring(0, finishTime.indexOf("(") - 1);
        }
        long start = dateFormat.parse(startTime).getTime();
        long finish = dateFormat.parse(finishTime).getTime();
        return new long[] { start, finish, 0, 0 };
      }
    }
    // This is a failed task attempt.
    return null;
  }

  private HadoopCounterHolder fetchCounter(Counters counters) {
    Map<CounterName, Long> counterMap = new EnumMap<CounterName, Long>(CounterName.class);
    for (Counters.Group group : counters) {
      for (Counter ctr : group) {
        CounterName cn = CounterName.getCounterFromName(ctr.getName());
        if (cn != null) {
          counterMap.put(cn, ctr.getValue());
        }
      }
    }
    return new HadoopCounterHolder(counterMap);
  }

  private void addJobToRetryList(HadoopJobData job) {
    if (_failedJobsInProgress.containsKey(job)) {
      // This is old retry job
      int retryLeft = _failedJobsInProgress.get(job);
      _failedJobsInProgress.remove(job);
      if (retryLeft == 0) {
        // Drop job on max retries
        logger.error("Drop job. Reason: reach max retry for job id=" + job.getJobId());
      } else {
        // Put this job back to failed job waiting list to be fetched again
        _failedJobsInWait.put(job, retryLeft);
      }
    } else {
      // This is new job to retry, add to wait list
      _failedJobsInWait.put(job, DEFAULT_RETRY);
    }
  }

  private List<HadoopJobData> fetchAllFailedJobs() {
    List<HadoopJobData> retryList = new ArrayList<HadoopJobData>();
    for (HadoopJobData job : _failedJobsInWait.keySet()) {
      int retry = _failedJobsInWait.get(job);
      retryList.add(job);
      _failedJobsInWait.remove(job);
      _failedJobsInProgress.put(job, retry - 1);
    }
    return retryList;
  }

  private void checkAndRemoveJobFromRetryList(HadoopJobData jobData) {
    if (_failedJobsInProgress.containsKey(jobData)) {
      _failedJobsInProgress.remove(jobData);
    }
  }

  private void clearJobData(HadoopJobData jobData) {
    jobData.setCounters(null).setJobConf(null).setMapperData(null).setReducerData(null);
  }

  // Return false if this is a real nodata received job ( 0 mapper & 0 reducer ) rather than a retired job
  private boolean checkRetiredAndFetchJobData(HadoopJobData jobData) throws IOException, AuthenticationException {
    String jobUrl = _jobtrackerHttpRoot + "jobdetails.jsp?jobid=" + jobData.getJobId();
    Document doc = ThreadContextMR1.fetchHtmlDoc(jobUrl);
    Elements hrefs = doc.select("a");
    String mapperUrl = null;
    String reducerUrl = null;
    String confUrl = null;

    // Parse all hrefs in job main page to find job conf page and mapper/reducer main page
    for (Element href : hrefs) {
      String hrefStr = href.attr("href");
      if (hrefStr.contains("jobconf_history.jsp")) {
        confUrl = _jobtrackerHttpRoot + hrefStr;
      }
      if (hrefStr.contains("taskType=MAP&status=SUCCESS")) {
        mapperUrl = _jobtrackerHttpRoot + hrefStr;
      }
      if (hrefStr.contains("taskType=REDUCE&status=SUCCESS")) {
        reducerUrl = _jobtrackerHttpRoot + hrefStr;
      }
    }

    if (mapperUrl == null || reducerUrl == null || confUrl == null) {
      // This is an real no data received job
      return false;
    }

    HadoopCounterHolder jobCounter = fetchJobCounterForRetiredJob(doc);
    Properties jobConf = fetchJobConfFromURL(confUrl);
    HadoopTaskData[] mapperData = fetchAllTaskDataForRetiredJob(mapperUrl, true);
    HadoopTaskData[] reducerData = fetchAllTaskDataForRetiredJob(reducerUrl, false);

    long startTime = fetchStartTimeForRetiredJob(doc, jobUrl);
    String username = jobConf.getProperty("user.name");
    String jobName = jobConf.getProperty("mapred.job.name");
    jobData.setUsername(username).setStartTime(startTime).setUrl(jobUrl).setJobName(jobName);
    jobData.setCounters(jobCounter).setJobConf(jobConf).setMapperData(mapperData).setReducerData(reducerData);
    return true;
  }

  // Fetch job counter from job's main page
  private HadoopCounterHolder fetchJobCounterForRetiredJob(Document doc) throws IOException, AuthenticationException {
    Map<CounterName, Long> counterMap = new EnumMap<CounterName, Long>(CounterName.class);
    Elements rows = doc.select("table").select("tr");
    for (Element row : rows) {
      Elements cells = row.select("> td");
      if (cells.size() == 5) {
        String countername = cells.get(1).text().trim();
        CounterName cn = CounterName.getCounterFromDisplayName(countername);
        if (cn != null) {
          counterMap.put(cn, Long.parseLong(cells.get(4).text().trim().replace(",", "")));
        }
      } else if (cells.size() == 4) {
        String countername = cells.get(0).text().trim();
        CounterName cn = CounterName.getCounterFromDisplayName(countername);
        if (cn != null) {
          counterMap.put(cn, Long.parseLong(cells.get(3).text().trim().replace(",", "")));
        }
      }
    }
    return new HadoopCounterHolder(counterMap);
  }

  // Fetch task counter from task's counter page
  public HadoopCounterHolder fetchTaskCounterForRetiredJob(String taskCounterUrl) throws IOException,
      AuthenticationException {
    Map<CounterName, Long> counterMap = new EnumMap<CounterName, Long>(CounterName.class);
    Document doc = ThreadContextMR1.fetchHtmlDoc(taskCounterUrl);
    Elements rows = doc.select("table").select("tr");
    for (Element row : rows) {
      Elements cells = row.select("> td");
      if (cells.size() == 3) {
        String countername = cells.get(1).text().trim();
        CounterName cn = CounterName.getCounterFromDisplayName(countername);
        if (cn != null) {
          counterMap.put(cn, Long.parseLong(cells.get(2).text().trim().replace(",", "")));
        }
      }
    }
    return new HadoopCounterHolder(counterMap);
  }

  // We fetch the start time of job's Setup task as the job's start time, shown in job's main page
  private long fetchStartTimeForRetiredJob(Document doc, String jobUrl) throws IOException {
    Elements rows = doc.select("table").select("tr");
    for (Element row : rows) {
      Elements cells = row.select("> td");
      if (cells.size() == 7 && cells.get(0).text().trim().equals("Setup")) {
        SimpleDateFormat dateFormat = ThreadContextMR1.getDateFormat();
        try {
          long time = dateFormat.parse(cells.get(5).text().trim()).getTime();
          return time;
        } catch (ParseException e) {
          throw new IOException("Error in fetching start time data from job page in URL : " + jobUrl);
        }
      }
    }
    throw new IOException("Unable to fetch start time data from job page in URL : " + jobUrl);
  }

  private HadoopTaskData[] fetchAllTaskDataForRetiredJob(String taskIndexPage, boolean isMapper) throws IOException,
      AuthenticationException {
    List<HadoopTaskData> taskList = new ArrayList<HadoopTaskData>();
    Document doc = ThreadContextMR1.fetchHtmlDoc(taskIndexPage);
    Elements hrefs = doc.select("a");
    for (Element href : hrefs) {
      String hrefStr = href.attr("href");
      if (hrefStr.contains("taskdetailshistory.jsp?")) {
        String taskUrl = _jobtrackerHttpRoot + hrefStr;
        HadoopTaskData tdata = fetchTaskDataForRetiredJob(taskUrl, isMapper);
        taskList.add(tdata);
      }
    }
    return taskList.toArray(new HadoopTaskData[taskList.size()]);
  }

  private HadoopTaskData fetchTaskDataForRetiredJob(String taskDetailUrl, boolean isMapper) throws IOException,
      AuthenticationException {
    Document doc = ThreadContextMR1.fetchHtmlDoc(taskDetailUrl);
    Elements rows = doc.select("table").select("tr");
    HadoopTaskData taskData = null;
    for (Element row : rows) {
      try {
        taskData = tryExtractTaskDataForRetiredJob(row, isMapper);
        if (taskData != null) {
          return taskData;
        }
      } catch (Exception e) {
        throw new IOException("Error in fetch task data from task detail page. TASK URL=" + taskDetailUrl, e);
      }
    }
    throw new IOException("No valid time data found from task detail page. TASK URL=" + taskDetailUrl);
  }

  private HadoopTaskData tryExtractTaskDataForRetiredJob(Element row, boolean isMapper) throws IOException,
      ParseException, AuthenticationException {
    Elements cells = row.select("> td");

    if (isMapper && cells.size() == 7) {
      SimpleDateFormat dateFormat = ThreadContextMR1.getDateFormatForRetiredJob();
      String c = cells.get(6).text().trim();
      if (c.matches("[^0][0-9]+")) {
        String startTime = cells.get(1).text().trim();
        String finishTime = cells.get(2).text().trim();
        if (finishTime.contains("(")) {
          finishTime = finishTime.substring(0, finishTime.indexOf("(") - 1);
        }
        long start = dateFormat.parse(startTime).getTime();
        long finish = dateFormat.parse(finishTime).getTime();
        long[] time = new long[] { start, finish, 0, 0 };
        String counterUrl = _jobtrackerHttpRoot + cells.get(6).select("a").attr("href");
        HadoopCounterHolder ctrholder = fetchTaskCounterForRetiredJob(counterUrl);
        return new HadoopTaskData(ctrholder, time);
      }
    } else if (!isMapper && cells.size() == 9) {
      SimpleDateFormat dateFormat = ThreadContextMR1.getDateFormatForRetiredJob();
      String c = cells.get(8).text().trim();
      if (c.matches("[^0][0-9]+")) {
        String startTime = cells.get(1).text().trim();
        String shuffleTime = cells.get(2).text().trim();
        String sortTime = cells.get(3).text().trim();
        String finishTime = cells.get(4).text().trim();
        if (shuffleTime.contains("(")) {
          shuffleTime = shuffleTime.substring(0, shuffleTime.indexOf("(") - 1);
        }
        if (sortTime.contains("(")) {
          sortTime = sortTime.substring(0, sortTime.indexOf("(") - 1);
        }
        if (finishTime.contains("(")) {
          finishTime = finishTime.substring(0, finishTime.indexOf("(") - 1);
        }
        long start = dateFormat.parse(startTime).getTime();
        long shuffle = dateFormat.parse(shuffleTime).getTime();
        long sort = dateFormat.parse(sortTime).getTime();
        long finish = dateFormat.parse(finishTime).getTime();
        long shuffleDuration = (shuffle - start);
        long sortDuration = (sort - shuffle);
        long[] time = new long[] { start, finish, shuffleDuration, sortDuration };
        String counterUrl = _jobtrackerHttpRoot + cells.get(8).select("a").attr("href");
        HadoopCounterHolder ctrholder = fetchTaskCounterForRetiredJob(counterUrl);
        return new HadoopTaskData(ctrholder, time);
      }
    }
    return null;
  }

}

final class ThreadContextMR1 {
  private static final Logger logger = Logger.getLogger(ThreadContextMR1.class);
  private static final ThreadLocal<Integer> _LOCAL_THREAD_ID = new ThreadLocal<Integer>();
  private static final ThreadLocal<JobClient> _LOCAL_JOB_CLIENT = new ThreadLocal<JobClient>();
  private static final ThreadLocal<AuthenticatedURL.Token> _LOCAL_AUTH_TOKEN =
      new ThreadLocal<AuthenticatedURL.Token>();
  private static final ThreadLocal<AuthenticatedURL> _LOCAL_AUTH_URL = new ThreadLocal<AuthenticatedURL>();
  private static final ThreadLocal<SimpleDateFormat> _LOCAL_DATE_FORMAT = new ThreadLocal<SimpleDateFormat>();
  private static final ThreadLocal<SimpleDateFormat> _LOCAL_DATE_FORMAT_FOR_RETIRED =
      new ThreadLocal<SimpleDateFormat>();
  private static final ThreadLocal<Long> _LOCAL_LAST_UPDATED = new ThreadLocal<Long>();
  private static final ThreadLocal<Long> _LOCAL_UPDATE_INTERVAL = new ThreadLocal<Long>();

  private ThreadContextMR1() {
  }

  public static void init(Configuration hadoopConf, int threadId) throws IOException {
    _LOCAL_JOB_CLIENT.set(new JobClient(new JobConf(hadoopConf)));
    _LOCAL_AUTH_TOKEN.set(new AuthenticatedURL.Token());
    _LOCAL_AUTH_URL.set(new AuthenticatedURL());
    _LOCAL_DATE_FORMAT.set(new SimpleDateFormat("d-MMM-yyyy HH:mm:ss"));
    _LOCAL_DATE_FORMAT_FOR_RETIRED.set(new SimpleDateFormat("d/MM HH:mm:ss"));
    _LOCAL_THREAD_ID.set(threadId);
    _LOCAL_LAST_UPDATED.set(System.currentTimeMillis());
    // Random an interval for each executor to avoid update token at the same time
    _LOCAL_UPDATE_INTERVAL.set(Statistics.MINUTE * 30 + new Random().nextLong() % (10 * Statistics.MINUTE));
    logger.info("Executor " + _LOCAL_THREAD_ID.get() + " update interval " + _LOCAL_UPDATE_INTERVAL.get() * 1.0
        / Statistics.MINUTE);
  }

  public static JobClient getClient() {
    return _LOCAL_JOB_CLIENT.get();
  }

  public static SimpleDateFormat getDateFormat() {
    return _LOCAL_DATE_FORMAT.get();
  }

  public static SimpleDateFormat getDateFormatForRetiredJob() {
    return _LOCAL_DATE_FORMAT_FOR_RETIRED.get();
  }

  public static Document fetchHtmlDoc(String url) throws IOException, AuthenticationException {
    HttpURLConnection conn = _LOCAL_AUTH_URL.get().openConnection(new URL(url), _LOCAL_AUTH_TOKEN.get());
    Document doc = Jsoup.parse(IOUtils.toString(conn.getInputStream()));
    return doc;
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
