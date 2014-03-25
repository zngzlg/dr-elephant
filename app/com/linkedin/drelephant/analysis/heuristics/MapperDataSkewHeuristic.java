package com.linkedin.drelephant.analysis.heuristics;

import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.hadoop.HadoopCounterHolder;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;

public class MapperDataSkewHeuristic extends GenericDataSkewHeuristic {
    private static final String message = HeuristicResult.addPossibleMapperResult("Data-skew");

    public MapperDataSkewHeuristic() {
        super(HadoopCounterHolder.CounterName.HDFS_BYTES_READ, message);
    }

    @Override
    protected HadoopTaskData[] getTasks(HadoopJobData data) {
        return data.getMapperData();
    }
}