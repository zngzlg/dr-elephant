package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.mapreduce.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.MapReduceTaskData;
import com.linkedin.drelephant.util.HeuristicConfigurationData;


public class MapperGCHeuristic extends GenericGCHeuristic {
  public static final String HEURISTIC_NAME = "Mapper GC";

  public MapperGCHeuristic(HeuristicConfigurationData heuristicConfData) {
    super(HEURISTIC_NAME, heuristicConfData);
  }

  @Override
  public String getHeuristicName() {
    return HEURISTIC_NAME;
  }

  @Override
  protected MapReduceTaskData[] getTasks(MapReduceApplicationData data) {
    return data.getMapperData();
  }
}
