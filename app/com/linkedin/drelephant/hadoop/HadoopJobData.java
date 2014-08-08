package com.linkedin.drelephant.hadoop;

import java.util.Properties;


public class HadoopJobData {
  private String jobId="";
  private String username = "";
  private String url = "";
  private String jobName = "";
  private long startTime = 0;
  private HadoopCounterHolder counterHolder;
  private HadoopTaskData[] mapperData;
  private HadoopTaskData[] reducerData;
  private Properties jobConf;

  public HadoopJobData setJobId(String jobId) {
    this.jobId = jobId;
    return this;
  }

  public HadoopJobData setJobName(String jobName) {
    this.jobName = jobName;
    return this;
  }

  public HadoopJobData setUsername(String username) {
    this.username = username;
    return this;
  }

  public HadoopJobData setStartTime(long startTime) {
    this.startTime = startTime;
    return this;
  }

  public HadoopJobData setUrl(String url) {
    this.url = url;
    return this;
  }

  public HadoopJobData setCounters(HadoopCounterHolder counterHolder) {
    this.counterHolder = counterHolder;
    return this;
  }

  public HadoopJobData setMapperData(HadoopTaskData[] mappers) {
    this.mapperData = mappers;
    return this;
  }

  public HadoopJobData setReducerData(HadoopTaskData[] reducers) {
    this.reducerData = reducers;
    return this;
  }

  public HadoopJobData setJobConf(Properties jobConf) {
    this.jobConf = jobConf;
    return this;
  }

  public HadoopCounterHolder getCounters() {
    return counterHolder;
  }

  public HadoopTaskData[] getMapperData() {
    return mapperData;
  }

  public HadoopTaskData[] getReducerData() {
    return reducerData;
  }

  public Properties getJobConf() {
    return jobConf;
  }

  public String getUsername() {
    return username;
  }

  public long getStartTime() {
    return startTime;
  }

  public String getUrl() {
    return url;
  }

  public String getJobName() {
    return jobName;
  }

  public String getJobId() {
    return jobId;
  }
}
