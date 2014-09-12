package com.linkedin.drelephant;

import java.util.Properties;

import org.apache.log4j.Logger;

import model.JobResult;

import com.linkedin.drelephant.hadoop.HadoopJobData;


/**
 * InfoExtractor is responsible for retrieving information and context about a
 * job from the job's configuration which will be leveraged by the UI
 */
public class InfoExtractor {
  private static final Logger logger = Logger.getLogger(InfoExtractor.class);
  private static final String AZK_URL_PREFIX = "azkaban.link";
  private static final String AZK_WORKFLOW_URL = "azkaban.link.workflow.url";
  private static final String AZK_JOB_URL = "azkaban.link.job.url";
  private static final String AZK_JOB_EXECUTION_URL = "azkaban.link.jobexec.url";
  private static final String AZK_EXECUTION_URL = "azkaban.link.execution.url";
  private static final String AZK_ATTEMPT_URL = "azkaban.link.attempt.url";
  private static final String AZK_URN_KEY = "azk.urn";

  void retrieveURLs(JobResult result, HadoopJobData jobData) {
    Properties jobConf = jobData.getJobConf();
    String jobId = jobData.getJobId();
    result.jobExecUrl = truncate(jobConf.getProperty(AZK_ATTEMPT_URL), jobId);
    // For jobs launched by Azkaban, we consider different attempts to be
    // different jobs
    result.jobUrl = truncate(jobConf.getProperty(AZK_JOB_URL), jobId);
    result.flowExecUrl = truncate(jobConf.getProperty(AZK_EXECUTION_URL), jobId);
    result.flowUrl = truncate(jobConf.getProperty(AZK_WORKFLOW_URL), jobId);
  }

  String truncate(String value, String jobId) {
    if (value != null && value.length() > JobResult.URL_LEN_LIMIT) {
      logger.info("Truncate long URL in job result for job: " + jobId + ". Original Url: " + value);
      value = value.substring(0, JobResult.URL_LEN_LIMIT);
    }
    return value;
  }
}
