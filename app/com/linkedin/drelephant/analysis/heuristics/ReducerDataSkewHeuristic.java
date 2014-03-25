package com.linkedin.drelephant.analysis.heuristics;

import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.hadoop.HadoopCounterHolder;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;

public class ReducerDataSkewHeuristic extends GenericDataSkewHeuristic {
    private static final String message = HeuristicResult.addPossibleReducerResult("Data-skew");

    public ReducerDataSkewHeuristic() {
        super(HadoopCounterHolder.CounterName.REDUCE_SHUFFLE_BYTES, message);
    }

    @Override
    protected HadoopTaskData[] getTasks(HadoopJobData data) {
        return data.getReducerData();
    }
}
