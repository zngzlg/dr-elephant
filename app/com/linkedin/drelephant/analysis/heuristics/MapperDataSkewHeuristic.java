package com.linkedin.drelephant.analysis.heuristics;

import com.linkedin.drelephant.hadoop.HadoopCounterHolder;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;

public class MapperDataSkewHeuristic extends GenericDataSkewHeuristic {
    public MapperDataSkewHeuristic() {
        super(HadoopCounterHolder.CounterName.HDFS_BYTES_READ, "Data-skew at Mapper side");
    }

    @Override
    protected HadoopTaskData[] getTasks(HadoopJobData data) {
        return data.getMapperData();
    }
}