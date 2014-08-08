package com.linkedin.drelephant;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import model.JobHeuristicResult;
import model.JobResult;
import model.JobType;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopSecurity;
import com.linkedin.drelephant.notifications.EmailThread;


public class ElephantRunner implements Runnable {
  private static final long WAIT_INTERVAL = 10 * 1000;
  private static final Logger logger = Logger.getLogger(ElephantRunner.class);
  private AtomicBoolean running = new AtomicBoolean(true);
  private EmailThread emailer = new EmailThread();
  private HadoopSecurity hadoopSecurity;
  private InfoExtractor urlRetriever = new InfoExtractor();

  @Override
  public void run() {
    logger.info("Dr.elephant has started");
    try {
      hadoopSecurity = new HadoopSecurity();
      hadoopSecurity.doAs(new PrivilegedAction<Void>() {
        @Override
        public Void run() {
          Constants.load();
          emailer.start();
          long lastRun;
          ElephantFetcher fetcher = null;

          try {
            // Tell which hadoop version from hadoop configuration,
            // and start fetcher accordingly
            Configuration hadoopConf = new Configuration();
            String framework = hadoopConf.get("mapreduce.framework.name");

            if (framework != null) {
              if (framework.equals("yarn")) {
                fetcher = new ElephantFetcherYarn(hadoopConf);
              } else if (framework.equals("classic")) {
                fetcher = new ElephantFetcherClassic(hadoopConf);
              } else {
                logger.error("mapreduce.framework.name must be either 'classic' or 'yarn'. Current value: "+framework);
                return null;
              }
            } else {
              if (hadoopConf.get("mapred.job.tracker.http.address") != null) {
                fetcher = new ElephantFetcherClassic(hadoopConf);
              } else {
                logger.error("Either mapreduce.framework.name or mapred.job.tracker.http.address must be set. Plseae check your configuration.");
                return null;
              }
            }

          } catch (IOException e) {
            logger.error("Error initializing dr elephant fetcher! ", e);
            return null;
          }

          while (running.get()) {
            lastRun = System.currentTimeMillis();

            logger.info("Fetching job list.....");

            try {
              hadoopSecurity.checkLogin();
            } catch (IOException e) {
              logger.info("Error with hadoop kerberos login", e);
              continue;
            }

            List<HadoopJobData> successJobs;
            try {
              successJobs = fetcher.fetchJobList();
            } catch (Exception e) {
              logger.error("Error fetching job list. Try again later...", e);
              continue;
            }

            logger.info(successJobs.size() + " jobs to analyse.");

            // Analyse all ready jobs
            for (HadoopJobData jobData : successJobs) {
              try {
                fetcher.fetchJobData(jobData);
                analyzeJob(jobData);
                fetcher.finishJob(jobData, true);
              } catch (Exception e) {
                logger.error("Error fetching job data. job id=" + jobData.getJobId(), e);
                fetcher.finishJob(jobData, false);
              }
            }
            logger.info("Finished all jobs. Waiting for refresh.");

            // Wait for long enough
            long nextRun = lastRun + WAIT_INTERVAL;
            long waitTime = nextRun - System.currentTimeMillis();
            while (running.get() && waitTime > 0) {
              try {
                Thread.sleep(waitTime);
              } catch (InterruptedException e) {
                logger.error("Thread interrupted", e);
              }
              waitTime = nextRun - System.currentTimeMillis();
            }
          }
          return null;
        }
      });
    } catch (IOException e) {
      logger.error("Error on Hadoop Security setup. Failed to login with Kerberos");
    }
  }

  private void analyzeJob(HadoopJobData jobData) {
    ElephantAnalyser analyser = ElephantAnalyser.instance();

    logger.info("Analyze job " + jobData.getJobId());

    HeuristicResult[] analysisResults = analyser.analyse(jobData);
    JobType jobType = analyser.getJobType(jobData);

    // Save to DB
    JobResult result = new JobResult();
    result.job_id = jobData.getJobId();
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
    urlRetriever.retrieveURLs(result, jobData);

    result.save();

    emailer.enqueue(result);
  }

  public void kill() {
    running.set(false);
    emailer.kill();
  }
}
