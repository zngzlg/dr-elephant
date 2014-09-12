package com.linkedin.drelephant.hadoop;

public class HadoopTaskData {
  private HadoopCounterHolder _counterHolder;
  private long _startTime = 0;
  private long _endTime = 0;
  private long _shuffleTime = 0;
  private long _sortTime = 0;
  private boolean _timed = false;

  public HadoopTaskData(HadoopCounterHolder counterHolder, long[] time) {
    this._counterHolder = counterHolder;
    this._startTime = time[0];
    this._endTime = time[1];
    this._shuffleTime = time[2];
    this._sortTime = time[3];
    this._timed = true;
  }

  public HadoopTaskData(HadoopCounterHolder counterHolder) {
    this._counterHolder = counterHolder;
  }

  public HadoopCounterHolder getCounters() {
    return _counterHolder;
  }

  public long getStartTime() {
    return _startTime;
  }

  public long getEndTime() {
    return _endTime;
  }

  public long getRunTime() {
    return _endTime - _startTime;
  }

  public long getExecutionTime() {
    return _endTime - _startTime - _shuffleTime - _sortTime;
  }

  public long getShuffleTime() {
    return _shuffleTime;
  }

  public long getSortTime() {
    return _sortTime;
  }

  public boolean timed() {
    return _timed;
  }
}
