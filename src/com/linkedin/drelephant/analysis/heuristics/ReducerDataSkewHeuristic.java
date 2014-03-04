package com.linkedin.drelephant.analysis.heuristics;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.hadoop.HadoopCounterHolder;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;
import com.linkedin.drelephant.math.Statistics;
import org.apache.commons.io.FileUtils;

public class ReducerDataSkewHeuristic implements Heuristic {
    @Override
    public HeuristicResult apply(HadoopJobData data) {
        HadoopTaskData[] reducers = data.getReducerData();

        long[] byte_counters = new long[reducers.length];

        for (int i = 0; i < reducers.length; i++) {
            byte_counters[i] = reducers[i].getCounters().get(HadoopCounterHolder.CounterName.REDUCE_SHUFFLE_BYTES);
        }

        int deviation = Statistics.deviates(byte_counters, 1024 * 1024, 0.5D);

        if (deviation > -1) {
            return buildError(reducers, Statistics.average(byte_counters), deviation, byte_counters[deviation]);
        }

        return HeuristicResult.SUCCESS;
    }

    private HeuristicResult buildError(HadoopTaskData[] mappers, long average, int deviation, long bytes) {
        String error = "Data-skew at reducer side. " +
                "\nSkewed reducer Task ID: " + mappers[deviation].getTaskId().toString() +
                "\nAverage reducer input: " + FileUtils.byteCountToDisplaySize(average) +
                "\nSkewed reducer input: " + FileUtils.byteCountToDisplaySize(bytes) + " (" + String.format("%.2fx", (float) bytes / (float) average) + ")";
        return new HeuristicResult(error, false);
    }
}
