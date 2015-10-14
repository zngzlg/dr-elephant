package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.mapreduce.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.MapReduceTaskData;


public class ReducerGCHeuristic extends GenericGCHeuristic {
  public static final String HEURISTIC_NAME = "Reducer GC";

  public ReducerGCHeuristic() {
    super(HEURISTIC_NAME);
  }

  @Override
  public String getHeuristicName() {
    return HEURISTIC_NAME;
  }

  @Override
  protected MapReduceTaskData[] getTasks(MapReduceApplicationData data) {
    return data.getReducerData();
  }
}
