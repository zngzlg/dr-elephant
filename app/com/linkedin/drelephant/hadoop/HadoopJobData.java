package com.linkedin.drelephant.hadoop;

import java.util.Properties;


public class HadoopJobData {
  private String _jobId = "";
  private String _username = "";
  private String _url = "";
  private String _jobName = "";
  private long _startTime = 0;
  private long _finishTime = 0;
  private HadoopCounterHolder _counterHolder;
  private HadoopTaskData[] _mapperData;
  private HadoopTaskData[] _reducerData;
  private Properties _jobConf;
  private boolean _isRetry = false;

  public HadoopJobData setRetry(boolean isRetry) {
    this._isRetry = isRetry;
    return this;
  }

  public HadoopJobData setJobId(String jobId) {
    this._jobId = jobId;
    return this;
  }

  public HadoopJobData setJobName(String jobName) {
    this._jobName = jobName;
    return this;
  }

  public HadoopJobData setUsername(String username) {
    this._username = username;
    return this;
  }

  public HadoopJobData setStartTime(long startTime) {
    this._startTime = startTime;
    return this;
  }

  public HadoopJobData setFinishTime(long finishTime) {
    this._finishTime = finishTime;
    return this;
  }

  public HadoopJobData setUrl(String url) {
    this._url = url;
    return this;
  }

  public HadoopJobData setCounters(HadoopCounterHolder counterHolder) {
    this._counterHolder = counterHolder;
    return this;
  }

  public HadoopJobData setMapperData(HadoopTaskData[] mappers) {
    this._mapperData = mappers;
    return this;
  }

  public HadoopJobData setReducerData(HadoopTaskData[] reducers) {
    this._reducerData = reducers;
    return this;
  }

  public HadoopJobData setJobConf(Properties jobConf) {
    this._jobConf = jobConf;
    return this;
  }

  public HadoopCounterHolder getCounters() {
    return _counterHolder;
  }

  public HadoopTaskData[] getMapperData() {
    return _mapperData;
  }

  public HadoopTaskData[] getReducerData() {
    return _reducerData;
  }

  public Properties getJobConf() {
    return _jobConf;
  }

  public String getUsername() {
    return _username;
  }

  public long getStartTime() {
    return _startTime;
  }

  public long getFinishTime() {
    return _finishTime;
  }

  public String getUrl() {
    return _url;
  }

  public String getJobName() {
    return _jobName;
  }

  public boolean isRetryJob() {
    return _isRetry;
  }

  public String getJobId() {
    return _jobId;
  }
}
