package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.mapreduce.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.MapReduceTaskData;


public class MapperMemoryHeuristic extends GenericMemoryHeuristic {
  public static final String HEURISTIC_NAME = "Mapper Memory";
  public static final String MAPPER_MEMORY_CONF = "mapreduce.map.memory.mb";

  public MapperMemoryHeuristic() {
    super(MAPPER_MEMORY_CONF, HEURISTIC_NAME);
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
