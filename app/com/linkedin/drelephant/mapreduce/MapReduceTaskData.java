package com.linkedin.drelephant.mapreduce;

public class MapReduceTaskData {
  private MapReduceCounterHolder _counterHolder;
  private long _totalTimeMs = 0;
  private long _shuffleTimeMs = 0;
  private long _sortTimeMs = 0;
  private boolean _timed = false;

  public MapReduceTaskData(MapReduceCounterHolder counterHolder, long[] time) {
    this._counterHolder = counterHolder;
    this._totalTimeMs = time[0];
    this._shuffleTimeMs = time[1];
    this._sortTimeMs = time[2];
    this._timed = true;
  }

  public MapReduceTaskData(MapReduceCounterHolder counterHolder) {
    this._counterHolder = counterHolder;
  }

  public MapReduceCounterHolder getCounters() {
    return _counterHolder;
  }

  public long getTotalRunTimeMs() {
    return _totalTimeMs;
  }

  public long getCodeExecutionTimeMs() {
    return _totalTimeMs - _shuffleTimeMs - _sortTimeMs;
  }

  public long getShuffleTimeMs() {
    return _shuffleTimeMs;
  }

  public long getSortTimeMs() {
    return _sortTimeMs;
  }

  public boolean timed() {
    return _timed;
  }
}
