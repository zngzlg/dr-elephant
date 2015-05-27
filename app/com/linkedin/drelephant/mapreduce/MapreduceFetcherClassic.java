package com.linkedin.drelephant.mapreduce;

import com.linkedin.drelephant.analysis.ElephantFetcher;
import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.mapreduce.HadoopCounterHolder.CounterName;
import com.linkedin.drelephant.math.Statistics;
import com.linkedin.drelephant.util.Utils;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.IOUtils;
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


public class MapreduceFetcherClassic implements ElephantFetcher<MapreduceApplicationData> {
  private static final Logger logger = Logger.getLogger(ElephantFetcher.class);
  private static final int DEFAULT_RETRY = 2;
  private JobConf _conf;

  private Map<MapreduceApplicationData, Integer> _failedJobsInWait = new ConcurrentHashMap<MapreduceApplicationData, Integer>();
  private Map<MapreduceApplicationData, Integer> _failedJobsInProgress = new ConcurrentHashMap<MapreduceApplicationData, Integer>();
  private String _jobtrackerHttpRoot;

  public MapreduceFetcherClassic() throws IOException {
    _conf = new JobConf();
    _jobtrackerHttpRoot = "http://" + _conf.get("mapred.job.tracker.http.address") + "/";
  }

  @Override
  public void init(int threadId) throws IOException {
    ThreadContextMR1.init(_conf, threadId);
  }

  @Override
  public MapreduceApplicationData fetchData(String id) throws IOException, AuthenticationException {
    MapreduceApplicationData jobData = new MapreduceApplicationData();
    jobData.setJobId(id);
    try {
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
        if (checkRetiredAndFetchJobData(jobData)) {
          // This is a retired job
          logger.info(jobData.getJobId() + " retired. Fetch data from web UI.");
          return jobData;
        }
        // This is a no data job
      }

      JobStatus status = job.getJobStatus();
      String username = status.getUsername();
      long startTime = status.getStartTime();
      // ALERT:
      //     In Hadoop-1 the getFinishTime() call is not there. Unfortunately, calling this method does not
      //     result in a compile error, neither a MethodNotFound exception at run time. The program just hangs.
      //     Since we don't have metrics (the only consumer of finish time as of now) in Hadoop-1, set the finish time
      //     to be the same as start time.
      long finishTime = startTime;
      String jobUrl = job.getTrackingURL();
      String jobName = job.getJobName();
      jobData.setUsername(username).setStartTime(startTime).setFinishTime(finishTime).setUrl(jobUrl).setJobName(jobName);

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

    } finally {
      ThreadContextMR1.updateAuthToken();
    }

    Utils.publishMetrics(jobData);

    return jobData;
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

  //This method return time array if successfully extracts data from table row (<tr>)
  //Return [totalMs,shuffleMs,sortMs] for reducers, [totalMs,0,0] for mappers
  private long[] tryExtractDetailFromRow(Element row, boolean isMapper) throws ParseException {
    Elements cells = row.select("> td");

    // For rows(<tr></tr>) in reducer task page with other than 12 cols(<td></td>),or 10 cols in mapper page,
    // they are not rows that contains time data
    if ((!isMapper && cells.size() != 12) || (isMapper && cells.size() != 10)) {
      return null;
    }

    boolean succeeded = cells.get(2).html().trim().equals("SUCCEEDED");
    if (succeeded) {
      if (!isMapper) {
        // reducer task. Get total/shuffle/sort time from time span. Html text format: [abs time(time span)]
        long shuffleTimeMs = parseTimeInMs(cells.get(5).html().trim());
        long sortTimeMs = parseTimeInMs(cells.get(6).html().trim());
        long totalTimeMs = parseTimeInMs(cells.get(7).html().trim());
        return new long[] { totalTimeMs, shuffleTimeMs, sortTimeMs };
      } else {
        // Mapper task. Get total time from time span
        long totalTimeMs = parseTimeInMs(cells.get(5).html().trim());
        return new long[] { totalTimeMs, 0, 0 };
      }
    }
    // This is a failed task attempt.
    return null;
  }

  private HadoopCounterHolder fetchCounter(Counters counters) {
    HadoopCounterHolder holder = new HadoopCounterHolder();
    for (Counters.Group group : counters) {
      for (Counter ctr : group) {
        CounterName cn = CounterName.getCounterFromName(ctr.getName());
        if (cn != null) {
          holder.set(cn.getGroupName(), cn.getName(), ctr.getValue());
        }
      }
    }
    return holder;
  }

  private void addJobToRetryList(MapreduceApplicationData job) {
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

  // Return false if this is a real nodata received job ( 0 mapper & 0 reducer ) rather than a retired job
  private boolean checkRetiredAndFetchJobData(MapreduceApplicationData jobData) throws IOException, AuthenticationException {
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
    // We can scrape the job tracker page to get the finish time, but in order to be consistent with the
    // non-retired jobs, we set the finish time to be same as start time here.
    long finishTime = startTime;
    String username = jobConf.getProperty("user.name");
    String jobName = jobConf.getProperty("mapred.job.name");
    jobData.setUsername(username).setStartTime(startTime).setFinishTime(finishTime).setUrl(jobUrl).setJobName(jobName);
    jobData.setCounters(jobCounter).setJobConf(jobConf).setMapperData(mapperData).setReducerData(reducerData);
    return true;
  }

  // Fetch job counter from job's main page
  private HadoopCounterHolder fetchJobCounterForRetiredJob(Document doc) throws IOException, AuthenticationException {
    HadoopCounterHolder holder = new HadoopCounterHolder();
    Elements rows = doc.select("table").select("tr");
    for (Element row : rows) {
      Elements cells = row.select("> td");
      if (cells.size() == 5) {
        String countername = cells.get(1).text().trim();
        CounterName cn = CounterName.getCounterFromDisplayName(countername);
        if (cn != null) {
          long value = Long.parseLong(cells.get(4).text().trim().replace(",", ""));
          holder.set(cn.getGroupName(), cn.getName(), value);
        }
      } else if (cells.size() == 4) {
        String countername = cells.get(0).text().trim();
        CounterName cn = CounterName.getCounterFromDisplayName(countername);
        if (cn != null) {
          long value = Long.parseLong(cells.get(3).text().trim().replace(",", ""));
          holder.set(cn.getGroupName(), cn.getName(), value);
        }
      }
    }
    return holder;
  }

  // Fetch task counter from task's counter page
  public HadoopCounterHolder fetchTaskCounterForRetiredJob(String taskCounterUrl) throws IOException,
      AuthenticationException {
    HadoopCounterHolder holder = new HadoopCounterHolder();
    Document doc = ThreadContextMR1.fetchHtmlDoc(taskCounterUrl);
    Elements rows = doc.select("table").select("tr");
    for (Element row : rows) {
      Elements cells = row.select("> td");
      if (cells.size() == 3) {
        String countername = cells.get(1).text().trim();
        CounterName cn = CounterName.getCounterFromDisplayName(countername);
        if (cn != null) {
          long value = Long.parseLong(cells.get(2).text().trim().replace(",", ""));
          holder.set(cn.getGroupName(), cn.getName(), value);
        }
      }
    }
    return holder;
  }

  // We fetch the start time of job's Setup task as the job's start time, shown in job's main page
  private long fetchStartTimeForRetiredJob(Document doc, String jobUrl) throws IOException {
    Elements rows = doc.select("table").select("tr");
    for (Element row : rows) {
      Elements cells = row.select("> td");
      if (cells.size() == 7 && cells.get(0).text().trim().equals("Setup")) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("d-MMM-yyyy HH:mm:ss");
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
      String c = cells.get(6).text().trim();
      if (c.matches("[^0][0-9]+")) {
        long totalTimeMs = parseTimeInMs(cells.get(2).html().trim());
        long[] time = new long[] { totalTimeMs, 0, 0 };
        String counterUrl = _jobtrackerHttpRoot + cells.get(6).select("a").attr("href");
        HadoopCounterHolder ctrholder = fetchTaskCounterForRetiredJob(counterUrl);
        return new HadoopTaskData(ctrholder, time);
      }
    } else if (!isMapper && cells.size() == 9) {
      String c = cells.get(8).text().trim();
      if (c.matches("[^0][0-9]+")) {
        long shuffleTimeMs = parseTimeInMs(cells.get(2).html().trim());
        long sortTimeMs = parseTimeInMs(cells.get(3).html().trim());
        long totalTimeMs = parseTimeInMs(cells.get(4).html().trim());
        long[] time = new long[] { totalTimeMs, shuffleTimeMs, sortTimeMs };
        String counterUrl = _jobtrackerHttpRoot + cells.get(8).select("a").attr("href");
        HadoopCounterHolder ctrholder = fetchTaskCounterForRetiredJob(counterUrl);
        return new HadoopTaskData(ctrholder, time);
      }
    }
    return null;
  }

  // Return time in ms from input date "Xhrs,Xmins,Xsecs"
  private long parseTimeInMs(String rawDateStr) {
    int hour = 0;
    int min = 0;
    int sec = 0;
    String dateStr = rawDateStr.substring(rawDateStr.indexOf('(') + 1, rawDateStr.indexOf(')'));
    String[] parts = dateStr.split(",");
    for (String part : parts) {
      if (part.endsWith("hrs")) {
        hour = Integer.parseInt(part.substring(0, part.indexOf('h')).trim());
      } else if (part.endsWith("mins")) {
        min = Integer.parseInt(part.substring(0, part.indexOf('m')).trim());
      } else if (part.endsWith("sec")) {
        sec = Integer.parseInt(part.substring(0, part.indexOf('s')).trim());
      }
    }
    return hour * Statistics.HOUR_IN_MS + min * Statistics.MINUTE_IN_MS + sec * Statistics.SECOND_IN_MS;
  }
}

final class ThreadContextMR1 {
  private static final Logger logger = Logger.getLogger(ThreadContextMR1.class);
  private static final ThreadLocal<Integer> _LOCAL_THREAD_ID = new ThreadLocal<Integer>();
  private static final ThreadLocal<JobClient> _LOCAL_JOB_CLIENT = new ThreadLocal<JobClient>();
  private static final ThreadLocal<AuthenticatedURL.Token> _LOCAL_AUTH_TOKEN =
      new ThreadLocal<AuthenticatedURL.Token>();
  private static final ThreadLocal<AuthenticatedURL> _LOCAL_AUTH_URL = new ThreadLocal<AuthenticatedURL>();
  private static final ThreadLocal<Long> _LOCAL_LAST_UPDATED = new ThreadLocal<Long>();
  private static final ThreadLocal<Long> _LOCAL_UPDATE_INTERVAL = new ThreadLocal<Long>();

  private ThreadContextMR1() {
  }

  public static void init(JobConf hadoopConf, int threadId) throws IOException {
    _LOCAL_JOB_CLIENT.set(new JobClient(hadoopConf));
    _LOCAL_AUTH_TOKEN.set(new AuthenticatedURL.Token());
    _LOCAL_AUTH_URL.set(new AuthenticatedURL());
    _LOCAL_THREAD_ID.set(threadId);
    _LOCAL_LAST_UPDATED.set(System.currentTimeMillis());
    // Random an interval for each executor to avoid update token at the same time
    _LOCAL_UPDATE_INTERVAL.set(Statistics.MINUTE_IN_MS * 30 + new Random().nextLong() % (3 * Statistics.MINUTE_IN_MS));
    logger.info("Executor " + _LOCAL_THREAD_ID.get() + " update interval " + _LOCAL_UPDATE_INTERVAL.get() * 1.0
        / Statistics.MINUTE_IN_MS);
  }

  public static JobClient getClient() {
    return _LOCAL_JOB_CLIENT.get();
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
