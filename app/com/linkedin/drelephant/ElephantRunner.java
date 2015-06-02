package com.linkedin.drelephant;

import com.linkedin.drelephant.analysis.AnalysisPromise;
import com.linkedin.drelephant.analysis.AnalysisProvider;
import com.linkedin.drelephant.analysis.AnalysisProviderHadoop1;
import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.analysis.AnalysisProviderHadoop2;
import com.linkedin.drelephant.notifications.EmailThread;
import com.linkedin.drelephant.util.Utils;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import model.JobResult;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;


public class ElephantRunner implements Runnable {
  private static final long WAIT_INTERVAL = 60 * 1000;
  private static final int EXECUTOR_NUM = 3;
  private static final Logger logger = Logger.getLogger(ElephantRunner.class);
  private AtomicBoolean _running = new AtomicBoolean(true);
  private long lastRun;
  private EmailThread _emailer = new EmailThread();
  private HadoopSecurity _hadoopSecurity;
  private ExecutorService _service;
  private BlockingQueue<AnalysisPromise> _jobQueue;
  private AnalysisProvider _analysisProvider;

  private void loadAnalysisProvider() {
    JobConf configuration = new JobConf();
    String hadoopVersion = Utils.getHadoopVersion(); configuration.get("mapreduce.framework.name");

    if (hadoopVersion.equals("yarn")) {
      _analysisProvider = new AnalysisProviderHadoop2();
    } else {
      _analysisProvider = new AnalysisProviderHadoop1();
    }

    if (_analysisProvider == null) {
      throw new RuntimeException("FutureProvider cannot be initialized correctly. Please check your configuration.");
    }

    try {
      _analysisProvider.configure(configuration);
    } catch (Exception e) {
      logger.error("Error occurred when configuring the analysis provider.");
      throw new RuntimeException(e);
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
          loadAnalysisProvider();

          _service = Executors.newFixedThreadPool(EXECUTOR_NUM);
          _jobQueue = new LinkedBlockingQueue<AnalysisPromise>();
          for (int i = 0; i < EXECUTOR_NUM; i++) {
            _service.submit(new ExecutorThread(i + 1, _jobQueue));
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

            List<AnalysisPromise> todos;
            try {
              todos = _analysisProvider.fetchPromises();
            } catch (Exception e) {
              logger.error("Error fetching job list. Try again later...", e);
              //Wait for a while before retry
              waitInterval();
              continue;
            }

            _jobQueue.addAll(todos);
            logger.info("Job queue size is " + _jobQueue.size());

            //Wait for a while before next fetch
            waitInterval();
          }
          logger.info("Main thread is terminated.");
          return null;
        }
      });
    } catch (IOException e) {
      logger.error(e.getMessage());
      logger.error(ExceptionUtils.getStackTrace(e));
      logger.error("Error on Hadoop Security setup. Failed to login with Kerberos");
    }
  }

  private class ExecutorThread implements Runnable {

    private int _threadId;
    private BlockingQueue<AnalysisPromise> _jobQueue;

    ExecutorThread(int threadNum, BlockingQueue<AnalysisPromise> jobQueue) {
      this._threadId = threadNum;
      this._jobQueue = jobQueue;
    }

    @Override
    public void run() {
      while (_running.get() && !Thread.currentThread().isInterrupted()) {
        AnalysisPromise promise = null;
        try {
          promise = _jobQueue.take();
          JobResult result = promise.getAnalysis();
          result.save();
          // TODO: how to test email sending?
          _emailer.enqueue(result);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        } catch (Exception e) {
          logger.error(e.getMessage());
          logger.error(ExceptionUtils.getStackTrace(e));

          if (promise != null && promise.retry()) {
            _analysisProvider.addIntoRetries(promise);
          } else {
            logger.error(
                "Drop analysis job. Reason: reached the max retries for application id = [" + promise.getAppId() + "].");
          }
        }
      }
      logger.info("Executor Thread" + _threadId + " is terminated.");
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
