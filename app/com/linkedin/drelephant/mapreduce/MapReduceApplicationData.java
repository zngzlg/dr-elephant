package com.linkedin.drelephant.mapreduce;

import com.linkedin.drelephant.analysis.HadoopApplicationData;
import java.util.Properties;


public class MapReduceApplicationData implements HadoopApplicationData {
  private String _jobId = "";
  private String _username = "";
  private String _url = "";
  private String _jobName = "";
  private long _startTime = 0;
  private long _finishTime = 0;
  private MapReduceCounterHolder _counterHolder;
  private MapReduceTaskData[] _mapperData;
  private MapReduceTaskData[] _reducerData;
  private Properties _jobConf;
  private boolean _isRetry = false;

  public MapReduceApplicationData setRetry(boolean isRetry) {
    this._isRetry = isRetry;
    return this;
  }

  public MapReduceApplicationData setJobId(String jobId) {
    this._jobId = jobId;
    return this;
  }

  public MapReduceApplicationData setJobName(String jobName) {
    this._jobName = jobName;
    return this;
  }

  public MapReduceApplicationData setUsername(String username) {
    this._username = username;
    return this;
  }

  public MapReduceApplicationData setStartTime(long startTime) {
    this._startTime = startTime;
    return this;
  }

  public MapReduceApplicationData setFinishTime(long finishTime) {
    this._finishTime = finishTime;
    return this;
  }

  public MapReduceApplicationData setUrl(String url) {
    this._url = url;
    return this;
  }

  public MapReduceApplicationData setCounters(MapReduceCounterHolder counterHolder) {
    this._counterHolder = counterHolder;
    return this;
  }

  public MapReduceApplicationData setMapperData(MapReduceTaskData[] mappers) {
    this._mapperData = mappers;
    return this;
  }

  public MapReduceApplicationData setReducerData(MapReduceTaskData[] reducers) {
    this._reducerData = reducers;
    return this;
  }

  public MapReduceApplicationData setJobConf(Properties jobConf) {
    this._jobConf = jobConf;
    return this;
  }

  public MapReduceCounterHolder getCounters() {
    return _counterHolder;
  }

  public MapReduceTaskData[] getMapperData() {
    return _mapperData;
  }

  public MapReduceTaskData[] getReducerData() {
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
