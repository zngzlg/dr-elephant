package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.mapreduce.HadoopCounterHolder;
import com.linkedin.drelephant.mapreduce.MapreduceApplicationData;
import com.linkedin.drelephant.mapreduce.HadoopTaskData;


public class MapperDataSkewHeuristic extends GenericDataSkewHeuristic {
  public static final String HEURISTIC_NAME = "Mapper Data Skew";

  public MapperDataSkewHeuristic() {
    super(HadoopCounterHolder.CounterName.HDFS_BYTES_READ, HEURISTIC_NAME);
  }

  @Override
  protected HadoopTaskData[] getTasks(MapreduceApplicationData data) {
    return data.getMapperData();
  }
}
