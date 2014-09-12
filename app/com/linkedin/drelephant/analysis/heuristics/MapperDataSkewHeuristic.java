package com.linkedin.drelephant.analysis.heuristics;

import com.linkedin.drelephant.hadoop.HadoopCounterHolder;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;


public class MapperDataSkewHeuristic extends GenericDataSkewHeuristic {
  public static final String HEURISTIC_NAME = "Mapper Data Skew";

  public MapperDataSkewHeuristic() {
    super(HadoopCounterHolder.CounterName.HDFS_BYTES_READ, HEURISTIC_NAME);
  }

  @Override
  protected HadoopTaskData[] getTasks(HadoopJobData data) {
    return data.getMapperData();
  }
}
