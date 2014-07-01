package com.linkedin.drelephant;

import java.util.Properties;

import model.JobResult;

import com.linkedin.drelephant.hadoop.HadoopJobData;

/**
 * InfoExtractor is responsible for retrieving information and context about a
 * job from the job's configuration which will be leveraged by the UI
 */
public class InfoExtractor {
  private static final String AZK_URL_PREFIX = "azkaban.link";
  private static final String AZK_WORKFLOW_URL = "azkaban.link.workflow.url";
  private static final String AZK_JOB_URL = "azkaban.link.job.url";
  private static final String AZK_JOB_EXECUTION_URL =
      "azkaban.link.jobexec.url";
  private static final String AZK_EXECUTION_URL = "azkaban.link.execution.url";
  private static final String AZK_ATTEMPT_URL = "azkaban.link.attempt.url";
  private static final String AZK_URN_KEY = "azk.urn";

  void retrieveURLs(JobResult result, HadoopJobData jobData) {
    Properties jobConf = jobData.getJobConf();
    result.jobExecUrl = jobConf.getProperty(AZK_ATTEMPT_URL);
    // For jobs launched by Azkaban, we consider different attempts to be
    // different jobs
    result.jobUrl = jobConf.getProperty(AZK_JOB_URL);
    result.flowExecUrl = jobConf.getProperty(AZK_EXECUTION_URL);
    result.flowUrl = jobConf.getProperty(AZK_WORKFLOW_URL);
  }
}
