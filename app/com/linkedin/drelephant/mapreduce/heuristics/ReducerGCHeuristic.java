package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.mapreduce.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.MapReduceTaskData;
import com.linkedin.drelephant.util.HeuristicConfigurationData;


public class ReducerGCHeuristic extends GenericGCHeuristic {
  public static final String HEURISTIC_NAME = "Reducer GC";

  public ReducerGCHeuristic(HeuristicConfigurationData _heuristicConfData) {
    super(HEURISTIC_NAME, _heuristicConfData);
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
