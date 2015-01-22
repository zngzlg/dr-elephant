package com.linkedin.drelephant;

import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.hadoop.HadoopCounterHolder;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopSecurity;
import com.linkedin.drelephant.notifications.EmailThread;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import model.JobHeuristicResult;
import model.JobResult;
import model.JobType;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import play.Play;


public class ElephantRunner implements Runnable {
  private static final long WAIT_INTERVAL = 60 * 1000;
  private static final int EXECUTOR_NUM = 3;
  private static final String OPT_METRICS_PUB_CONF = "metrics.publisher-conf";
  private static final Logger logger = Logger.getLogger(ElephantRunner.class);
  private AtomicBoolean _running = new AtomicBoolean(true);
  private long lastRun;
  private EmailThread _emailer = new EmailThread();
  private final DaliMetricsAPI.MetricsPublisher _metricsPublisher;
  private HadoopSecurity _hadoopSecurity;
  private InfoExtractor _urlRetriever = new InfoExtractor();
  private ExecutorService _service;
  private BlockingQueue<HadoopJobData> _jobQueue;

  public ElephantRunner() {
    // The getFile() API of Play returns a File object whether or not the actual file exists.
    String metricsPublisherConfPath = Play.application().configuration().getString(OPT_METRICS_PUB_CONF);
    if (metricsPublisherConfPath == null) {
      logger.info("Metrics publisher not configured. No metrics will be published");
      _metricsPublisher = null;
    } else {
        _metricsPublisher = DaliMetricsAPI.HDFSMetricsPublisher.createFromXml(metricsPublisherConfPath);
      if (_metricsPublisher == null) {
        logger.info("No metrics will be published");
      }
    }
  }

  @Override
  public void run() {
    logger.info("Dr.elephant has started");
    try {
      _hadoopSecurity = new HadoopSecurity();
      _hadoopSecurity.doAs(new PrivilegedAction<Void>() {
        @Override
        public Void run() {
          Constants.load();
          _emailer.start();
          ElephantFetcher fetcher = null;

          try {
            // Tell which hadoop version from hadoop configuration,
            // and start fetcher accordingly
            JobConf hadoopConf = new JobConf();
            String framework = hadoopConf.get("mapreduce.framework.name");

            if (framework != null) {
              if (framework.equals("yarn")) {
                fetcher = new ElephantFetcherYarn(hadoopConf);
              } else if (framework.equals("classic")) {
                fetcher = new ElephantFetcherClassic(hadoopConf);
              } else {
                logger.error("mapreduce.framework.name must be either 'classic' or 'yarn'. Current: " + framework);
                return null;
              }
            } else {
              if (hadoopConf.get("mapred.job.tracker.http.address") != null) {
                fetcher = new ElephantFetcherClassic(hadoopConf);
              } else {
                logger.error("Either mapreduce.framework.name or mapred.job.tracker.http.address must be set.");
                logger.error("Plseae check your configuration.");
                return null;
              }
            }

            logger.info("Initializing fetcher in main thread 0");
            fetcher.init(0);

          } catch (IOException e) {
            logger.error("Error initializing dr elephant fetcher in main thread", e);
            return null;
          }

          _service = Executors.newFixedThreadPool(EXECUTOR_NUM);
          _jobQueue = new LinkedBlockingQueue<HadoopJobData>();

          for (int i = 0; i < EXECUTOR_NUM; i++) {
            _service.submit(new ExecutorThread(i + 1, _jobQueue, fetcher));
          }

          try {
            ElephantAnalyser.init();
          } catch (Exception e) {
            logger.error("Error loading pluggable heuristics. ", e);
            return null;
          }

          while (_running.get() && !Thread.currentThread().isInterrupted()) {
            lastRun = System.currentTimeMillis();

            logger.info("Fetching job list.....");

            try {
              _hadoopSecurity.checkLogin();
            } catch (IOException e) {
              logger.info("Error with hadoop kerberos login", e);
              //Wait for a while before retry
              waitInterval();
              continue;
            }

            List<HadoopJobData> successJobs;
            try {
              successJobs = fetcher.fetchJobList();
            } catch (Exception e) {
              logger.error("Error fetching job list. Try again later...", e);
              //Wait for a while before retry
              waitInterval();
              continue;
            }

            _jobQueue.addAll(successJobs);
            logger.info("Job queue size is " + _jobQueue.size());

            //Wait for a while before next fetch
            waitInterval();
          }
          logger.info("Main thread is terminated.");
          return null;
        }
      });
    } catch (IOException e) {
      logger.error("Error on Hadoop Security setup. Failed to login with Kerberos");
    }
  }

  private class ExecutorThread implements Runnable {

    private int _threadId;
    private BlockingQueue<HadoopJobData> _jobQueue;
    private ElephantFetcher _fetcher;

    ExecutorThread(int threadNum, BlockingQueue<HadoopJobData> jobQueue, ElephantFetcher fetcher) {
      this._threadId = threadNum;
      this._jobQueue = jobQueue;
      this._fetcher = fetcher;
    }

    @Override
    public void run() {
      try {
        logger.info("Initializing fetcher in executor " + _threadId);
        this._fetcher.init(_threadId);
      } catch (IOException e) {
        logger.error("Error initialize fetcher in executor " + _threadId, e);
      }
      while (_running.get() && !Thread.currentThread().isInterrupted()) {
        HadoopJobData jobData = null;
        try {
          jobData = _jobQueue.take();
          _fetcher.fetchJobData(jobData);
          analyzeJob(jobData, _threadId);
          _fetcher.finishJob(jobData, true);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        } catch (Exception e) {
          logger.error("Error analyzing " + jobData.getJobId(), e);
          _fetcher.finishJob(jobData, false);
        }
      }
      logger.info("Executor Thread" + _threadId + " is terminated.");
    }
  }

  private void analyzeJob(HadoopJobData jobData, int execThreadNum) {
    ElephantAnalyser analyser = ElephantAnalyser.instance();
    logger.info("Analyze " + jobData.getJobId() + " by executor " + execThreadNum);

    HeuristicResult[] analysisResults = analyser.analyse(jobData);
    JobType jobType = analyser.getJobType(jobData);

    // Save to DB
    JobResult result = new JobResult();
    result.jobId = jobData.getJobId();
    result.url = jobData.getUrl();
    result.username = jobData.getUsername();
    result.startTime = jobData.getStartTime();
    result.analysisTime = System.currentTimeMillis();
    result.jobName = jobData.getJobName();
    result.jobType = jobType;

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
    _urlRetriever.retrieveURLs(result, jobData);

    if (_metricsPublisher != null) {
      publishMetrics(jobData);
    }

    result.save();

    _emailer.enqueue(result);
  }

  private void publishMetrics(HadoopJobData jobData) {
    Properties jobConf = jobData.getJobConf();
    // We may have something to publish, but we don't know until we have iterated through the counters that we have.
    // We assume that we need to publish something until we find out we don't.
    DaliMetricsAPI.JobProperties jobProperties = new DaliMetricsAPI.JobProperties(jobConf);
    if (jobProperties.getCountersToPublish().isEmpty()) {
      // Nothing to do
      return;
    }
    DaliMetricsAPI.EventContext eventContext = new DaliMetricsAPI.EventContext(jobData.getJobName(),
        jobData.getJobId(),
        jobData.getStartTime(),
        jobData.getFinishTime());
    DaliMetricsAPI.HadoopCounters metricsEvent = new DaliMetricsAPI.HadoopCounters(eventContext, jobProperties);

    HadoopCounterHolder counterHolder = jobData.getCounters();
    Set<String> groupNames = counterHolder.getGroupNames();
    for (String group : groupNames) {
      Map<String, Long> counters = counterHolder.getAllCountersInGroup(group);
      for (Map.Entry<String, Long> entry : counters.entrySet()) {
        String counterName = entry.getKey();
        Long value = entry.getValue();
        metricsEvent.addCounter(group, counterName, value);
      }
    }
    if (metricsEvent.getNumCounters() == 0) {
      // The counters that were configured were not collected in HadoopCounterHolder.
      return;
    }
    IndexedRecord event = metricsEvent.build();
    try {
      _metricsPublisher.publish(event);
    } catch (IOException e) {
      // The lower level should have logged a message.
      // A checked exception from the publish() call should mean that the event was not formed correctly for some reason.
      // Aside from a code bug, the most common reason for this will probably be that some mandatory fields in the
      // event were missing.
      // Could also mean that some derived values (e.g. hostname to URL, execId translation to an integer, etc.)
      // may have failed. There is little we can do at this point to fix those, so ignore the exception.
      logger.warn("Publish failed:" + e);
    }
  }

  private void waitInterval() {
    // Wait for long enough
    long nextRun = lastRun + WAIT_INTERVAL;
    long waitTime = nextRun - System.currentTimeMillis();

    if (waitTime <= 0) {
      return;
    }

    try {
      Thread.sleep(waitTime);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public void kill() {
    _running.set(false);
    _emailer.kill();
    _service.shutdownNow();
  }
}