package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.mapreduce.MapReduceCounterHolder;
import com.linkedin.drelephant.mapreduce.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.MapReduceTaskData;


public class ReducerDataSkewHeuristic extends GenericDataSkewHeuristic {
  public static final String HEURISTIC_NAME = "Reducer Data Skew";

  public ReducerDataSkewHeuristic() {
    super(MapReduceCounterHolder.CounterName.REDUCE_SHUFFLE_BYTES, HEURISTIC_NAME);
  }

  @Override
  protected MapReduceTaskData[] getTasks(MapReduceApplicationData data) {
    return data.getReducerData();
  }
}
