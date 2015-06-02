package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.mapreduce.MapReduceCounterHolder;
import com.linkedin.drelephant.mapreduce.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.MapReduceTaskData;


public class MapperDataSkewHeuristic extends GenericDataSkewHeuristic {
  public static final String HEURISTIC_NAME = "Mapper Data Skew";

  public MapperDataSkewHeuristic() {
    super(MapReduceCounterHolder.CounterName.HDFS_BYTES_READ, HEURISTIC_NAME);
  }

  @Override
  protected MapReduceTaskData[] getTasks(MapReduceApplicationData data) {
    return data.getMapperData();
  }
}
