package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.mapreduce.HadoopCounterHolder;
import com.linkedin.drelephant.mapreduce.MapreduceApplicationData;
import com.linkedin.drelephant.mapreduce.HadoopTaskData;


public class ReducerDataSkewHeuristic extends GenericDataSkewHeuristic {
  public static final String HEURISTIC_NAME = "Reducer Data Skew";

  public ReducerDataSkewHeuristic() {
    super(HadoopCounterHolder.CounterName.REDUCE_SHUFFLE_BYTES, HEURISTIC_NAME);
  }

  @Override
  protected HadoopTaskData[] getTasks(MapreduceApplicationData data) {
    return data.getReducerData();
  }
}
