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
import org.apache.hadoop.mapred.*;
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
import java.util.Set;


public class ElephantFetcherClassic implements ElephantFetcher {
  private static final Logger logger = Logger.getLogger(ElephantFetcher.class);
  private static SimpleDateFormat dateFormat = new SimpleDateFormat("d-MMM-yyyy HH:mm:ss");

  private JobClient jobClient;
  private Set<String> previousJobs = new HashSet<String>();
  private boolean firstRun = true;

  public ElephantFetcherClassic(Configuration hadoopConf) throws IOException {
    init(hadoopConf);
  }

  private void init(Configuration hadoopConf) throws IOException {
    logger.info("Connecting to the jobtracker");
    jobClient = new JobClient(new JobConf(hadoopConf));
  }

  public List<HadoopJobData> fetchJobList() throws IOException {
    JobStatus[] result = null;

    result = jobClient.getAllJobs();
    if (result == null) {
      throw new IOException("Error fetching joblist from jobtracker.");
    }

    Set<String> successJobs = filterSuccessfulJobs(result);
    successJobs = filterPreviousJobs(successJobs, previousJobs);

    List<HadoopJobData> jobList = new ArrayList<HadoopJobData>();
    for (String jobId : successJobs) {
      jobList.add(new HadoopJobData().setJobId(jobId));
    }
    return jobList;
  }

  public void finishJob(HadoopJobData jobData, boolean success) {
    if (success) {
      previousJobs.add(jobData.getJobId());
    }
  }

  public void fetchJobData(HadoopJobData jobData) throws IOException, AuthenticationException {
    JobID job_id = JobID.forName(jobData.getJobId());

    RunningJob job = getJob(job_id);
    if (job == null) {
      throw new IOException("Unable to fetch job data from Jobtracker, job id = " + job_id);
    }

    JobStatus status = job.getJobStatus();
    String username = status.getUsername();
    long startTime = status.getStartTime();
    String jobUrl = job.getTrackingURL();
    String jobName = job.getJobName();

    HadoopCounterHolder counterHolder = fetchCounter(job.getCounters());

    TaskReport[] mapperTasks = getMapTaskReports(job_id);
    TaskReport[] reducerTasks = getReduceTaskReports(job_id);
    String jobTrackingUrl = job.getTrackingURL();
    int sampleSize = Constants.SHUFFLE_SORT_MAX_SAMPLE_SIZE;

    HadoopTaskData[] mappers = new HadoopTaskData[mapperTasks.length];
    Statistics.shuffleArraySample(mapperTasks, sampleSize);
    for (int i = 0; i < mapperTasks.length; i++) {
      mappers[i] = fetchTaskData(jobTrackingUrl, mapperTasks[i], false, (i < sampleSize));
    }

    HadoopTaskData[] reducers = new HadoopTaskData[reducerTasks.length];
    Statistics.shuffleArraySample(reducerTasks, sampleSize);
    for (int i = 0; i < reducerTasks.length; i++) {
      reducers[i] = fetchTaskData(jobTrackingUrl, reducerTasks[i], true, (i < sampleSize));
    }

    Properties jobConf = getJobConf(job);

    jobData.setUsername(username).setStartTime(startTime).setUrl(jobUrl).setJobName(jobName).setCounters(counterHolder)
        .setMapperData(mappers).setReducerData(reducers).setJobConf(jobConf);

  }

  private RunningJob getJob(JobID job_id) throws IOException {
    return jobClient.getJob(job_id);
  }

  private TaskReport[] getMapTaskReports(JobID job_id) throws IOException {
    return jobClient.getMapTaskReports(job_id);
  }

  private TaskReport[] getReduceTaskReports(JobID job_id) throws IOException {
    return jobClient.getReduceTaskReports(job_id);
  }

  private Properties getJobConf(RunningJob job) throws IOException, AuthenticationException {
    Properties properties = new Properties();
    String jobconfUrl = getJobconfUrl(job);
    if (jobconfUrl == null) {
      return properties;
    }

    URL url = new URL(jobconfUrl);
    AuthenticatedURL.Token token = new AuthenticatedURL.Token();
    HttpURLConnection conn = new AuthenticatedURL().openConnection(url, token);
    String data = IOUtils.toString(conn.getInputStream());
    Document doc = Jsoup.parse(data);
    Elements rows = doc.select("table").select("tr");
    for (int i = 1; i < rows.size(); i++) {
      Element row = rows.get(i);
      Elements cells = row.select("> td");
      if (cells.size() == 2) {
        String key = cells.get(0).text().trim();
        String value = cells.get(1).text().trim();
        properties.put(key, value);
      }
    }
    return properties;
  }

  private String getJobconfUrl(RunningJob job) {
    String jobDetails = job.getTrackingURL();
    String root = jobDetails.substring(0, jobDetails.indexOf("jobdetails.jsp"));
    return root + "jobconf.jsp?jobid=" + job.getID().toString();
  }

  private Set<String> filterSuccessfulJobs(JobStatus[] jobs) {
    Set<String> successJobs = new HashSet<String>();
    for (JobStatus job : jobs) {
      if (job.getRunState() == JobStatus.SUCCEEDED && job.isJobComplete()) {
        successJobs.add(job.getJobID().toString());
      }
    }
    return successJobs;
  }

  private Set<String> filterPreviousJobs(Set<String> jobs, Set<String> previousJobs) {
    logger.info("Cleaning up previous runs.");
    // On first run, check against DB
    if (firstRun) {
      Set<String> newJobs = new HashSet<String>();
      for (String jobId : jobs) {
        JobResult prevResult = JobResult.find.byId(jobId);
        if (prevResult == null) {
          // Job not found, add to new jobs list
          newJobs.add(jobId);
        } else {
          // Job found, add to old jobs list
          previousJobs.add(jobId);
        }
      }
      jobs = newJobs;
      firstRun = false;
    } else {
      // Remove untracked jobs
      previousJobs.retainAll(jobs);
      // Remove previously analysed jobs
      jobs.removeAll(previousJobs);
    }
    return jobs;
  }

  private HadoopTaskData fetchTaskData(String jobDetailUrl, TaskReport task, boolean isReducer, boolean sampled)
      throws IOException, AuthenticationException {

    HadoopCounterHolder taskCounter = fetchCounter(task.getCounters());

    if (!sampled) {
      return new HadoopTaskData(taskCounter);
    }

    String taskDetailsUrl = getTaskDetailsPage(jobDetailUrl, task.getTaskID().toString());
    long[] time = fetchTaskDetails(taskDetailsUrl, isReducer);

    return new HadoopTaskData(taskCounter, time);
  }

  private String getTaskDetailsPage(String jobDetails, String taskId) {
    String root = jobDetails.substring(0, jobDetails.indexOf("jobdetails.jsp"));
    return root + "taskdetails.jsp?tipid=" + taskId.toString();
  }

  private long[] fetchTaskDetails(String taskDetailUrl, boolean isReducer) throws IOException, AuthenticationException {

    URL url = new URL(taskDetailUrl);
    AuthenticatedURL.Token token = new AuthenticatedURL.Token();
    HttpURLConnection conn = new AuthenticatedURL().openConnection(url, token);
    String data = IOUtils.toString(conn.getInputStream());
    Document doc = Jsoup.parse(data);
    Elements rows = doc.select("table").select("tr");
    long[] time = null;
    for (int i = 1; i < rows.size(); i++) {
      Element row = rows.get(i);
      try {
        time = tryExtractDetailFromRow(row, isReducer);
        if (time != null) {
          return time;
        }
      } catch (Exception e) {
        throw new IOException("Error in fetch task data from task detail page. TASK URL=" + taskDetailUrl, e);
      }
    }
    throw new IOException("No valid time data found from task detail page. TASK URL=" + taskDetailUrl);
  }

  //Return shuffle sort time if successfully extracted data from row
  private long[] tryExtractDetailFromRow(Element row, boolean isReducer) throws ParseException {
    Elements cells = row.select("> td");

    // For rows(<tr></tr>) in reducer task page with other than 12 cols(<td></td>),or 10 cols in mapper page,
    // they are not rows that contains time data
    if ((isReducer && cells.size() != 12) || (!isReducer && cells.size() != 10)) {
      return null;
    }

    boolean succeeded = cells.get(2).html().trim().equals("SUCCEEDED");
    if (succeeded) {
      if (isReducer) {
        // Fetch time info from reducer task page
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
        // Fetch time info from mapper task page
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
    return null;
  }

  private HadoopCounterHolder fetchCounter(Counters counters) {
    Map<CounterName, Long> counterMap = new EnumMap<CounterName, Long>(CounterName.class);
    for (CounterName counterName : CounterName.values()) {
      counterMap.put(counterName, readCounter(counterName, counters));
    }
    return new HadoopCounterHolder(counterMap);
  }

  private long readCounter(CounterName counterName, Counters counters) {
    String groupName = counterName.getGroup().getName();
    Counters.Group group = counters.getGroup(groupName);
    if (group == null) {
      return 0;
    }
    Counters.Counter counter = group.getCounterForName(counterName.getName());
    if (counter == null) {
      return 0;
    }
    return counter.getValue();
  }
}
