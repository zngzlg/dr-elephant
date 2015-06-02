package com.linkedin.drelephant.util;

import com.linkedin.drelephant.analysis.HadoopApplicationData;
import com.linkedin.drelephant.spark.SparkApplicationData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import model.JobResult;

import com.linkedin.drelephant.mapreduce.MapreduceApplicationData;


/**
 * InfoExtractor is responsible for retrieving information and context about a
 * job from the job's configuration which will be leveraged by the UI
 */
public class InfoExtractor {
  private static final Logger logger = Logger.getLogger(InfoExtractor.class);
  private static final String SPARK_EXTRA_JAVA_OPTIONS = "spark.driver.extraJavaOptions";

  private static final String AZKABAN_WORKFLOW_URL = "azkaban.link.workflow.url";
  private static final String AZKABAN_JOB_URL = "azkaban.link.job.url";
  private static final String AZKABAN_EXECUTION_URL = "azkaban.link.execution.url";
  private static final String AZKABAN_ATTEMPT_URL = "azkaban.link.attempt.url";

  // TODO: this utils class is not ideal, probably should merge retrieve URLs logics directly into the data interface?
  public static void retrieveURLs(JobResult result, HadoopApplicationData data) {
    if (data instanceof MapreduceApplicationData) {
      retrieveURLs(result, (MapreduceApplicationData) data);
    } else if (data instanceof SparkApplicationData) {
      retrieveURLs(result, (SparkApplicationData) data);
    }
  }

  public static void retrieveURLs(JobResult result, MapreduceApplicationData appData) {
    Properties jobConf = appData.getConf();
    String jobId = appData.getJobId();
    result.jobExecUrl = truncate(jobConf.getProperty(AZKABAN_ATTEMPT_URL), jobId);
    // For jobs launched by Azkaban, we consider different attempts to be
    // different jobs
    result.jobUrl = truncate(jobConf.getProperty(AZKABAN_JOB_URL), jobId);
    result.flowExecUrl = truncate(jobConf.getProperty(AZKABAN_EXECUTION_URL), jobId);
    result.flowUrl = truncate(jobConf.getProperty(AZKABAN_WORKFLOW_URL), jobId);
  }

  public static void retrieveURLs(JobResult result, SparkApplicationData appData) {
    String prop = appData.getEnvironmentData().getSparkProperty(SPARK_EXTRA_JAVA_OPTIONS);
    if (prop != null) {
      try {
        Map<String, String> options = Utils.parseJavaOptions(prop);

        List<String> s = new ArrayList<String>();
        for (Map.Entry<String, String> e : options.entrySet()) {
          s.add(e.getKey() + "->" + e.getValue());
        }
        logger.info("Parsed options:" + StringUtils.join(s, ","));

        result.jobExecUrl = unescapeString(options.get(AZKABAN_ATTEMPT_URL));
        result.jobUrl = unescapeString(options.get(AZKABAN_JOB_URL));
        result.flowExecUrl = unescapeString(options.get(AZKABAN_EXECUTION_URL));
        result.flowUrl = unescapeString(options.get(AZKABAN_WORKFLOW_URL));
      } catch (IllegalArgumentException e) {
        logger.error("Encountered error while parsing java options into urls: " + e.getMessage());
      }
    } else {
      logger.error("Unable to retrieve azkaban urls from application + [" +
          appData.getGeneralData().getApplicationId() + "] it does not contain [" + SPARK_EXTRA_JAVA_OPTIONS
          + "] property in its spark properties.");
    }
  }

  /**
   * A temporary solution that SPARK 1.2 need to escape '&' with '\&' in its javaOptions.
   * This is the reverse process that recovers the escaped string.
   *
   * @param s The string to unescape
   * @return The original string
   */
  private static String unescapeString(String s) {
    if (s == null) {
      return null;
    }
    return s.replaceAll("\\\\\\&", "\\&");
  }

  public static String truncate(String value, String jobId) {
    if (value != null && value.length() > JobResult.URL_LEN_LIMIT) {
      logger.info("Truncate long URL in job result for job: " + jobId + ". Original Url: " + value);
      value = value.substring(0, JobResult.URL_LEN_LIMIT);
    }
    return value;
  }
}
