package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.mapreduce.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.MapReduceTaskData;


public class ReducerMemoryHeuristic extends GenericMemoryHeuristic {
  public static final String HEURISTIC_NAME = "Reducer Memory";
  public static final String REDUCER_MEMORY_CONF = "mapreduce.reduce.memory.mb";

  public ReducerMemoryHeuristic() {
    super(REDUCER_MEMORY_CONF, HEURISTIC_NAME);
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
