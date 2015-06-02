package com.linkedin.drelephant.mapreduce;

import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.analysis.HadoopApplicationData;
import java.util.Properties;


public class MapreduceApplicationData implements HadoopApplicationData {
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

  public MapreduceApplicationData setRetry(boolean isRetry) {
    this._isRetry = isRetry;
    return this;
  }

  public MapreduceApplicationData setJobId(String jobId) {
    this._jobId = jobId;
    return this;
  }

  public MapreduceApplicationData setJobName(String jobName) {
    this._jobName = jobName;
    return this;
  }

  public MapreduceApplicationData setUsername(String username) {
    this._username = username;
    return this;
  }

  public MapreduceApplicationData setStartTime(long startTime) {
    this._startTime = startTime;
    return this;
  }

  public MapreduceApplicationData setFinishTime(long finishTime) {
    this._finishTime = finishTime;
    return this;
  }

  public MapreduceApplicationData setUrl(String url) {
    this._url = url;
    return this;
  }

  public MapreduceApplicationData setCounters(HadoopCounterHolder counterHolder) {
    this._counterHolder = counterHolder;
    return this;
  }

  public MapreduceApplicationData setMapperData(HadoopTaskData[] mappers) {
    this._mapperData = mappers;
    return this;
  }

  public MapreduceApplicationData setReducerData(HadoopTaskData[] reducers) {
    this._reducerData = reducers;
    return this;
  }

  public MapreduceApplicationData setJobConf(Properties jobConf) {
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

  @Override
  public String getUid() {
    return _jobId;
  }

  @Override
  public Properties getConf() {
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

  @Override
  public String toString() {
    return "id: " + getJobId() + ", name:" + getJobName();
  }
}
