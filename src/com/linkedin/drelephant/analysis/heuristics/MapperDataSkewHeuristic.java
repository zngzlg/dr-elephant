package com.linkedin.drelephant.analysis.heuristics;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.hadoop.HadoopCounterHolder;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;
import com.linkedin.drelephant.math.Statistics;
import org.apache.commons.io.FileUtils;

public class MapperDataSkewHeuristic implements Heuristic {
    @Override
    public HeuristicResult apply(HadoopJobData data) {
        HadoopTaskData[] mappers = data.getMapperData();

        long[] byte_counters = new long[mappers.length];

        for (int i = 0; i < mappers.length; i++) {
            byte_counters[i] = mappers[i].getCounters().get(HadoopCounterHolder.CounterName.HDFS_BYTES_READ);
        }

        int deviation = Statistics.deviates(byte_counters, 1024 * 1024, 0.5D);

        if (deviation > -1) {
            return buildError(mappers, Statistics.average(byte_counters), deviation, byte_counters[deviation]);
        }

        return HeuristicResult.SUCCESS;
    }

    private HeuristicResult buildError(HadoopTaskData[] mappers, long average, int deviation, long bytes) {

        String error = "Data-skew at mapper side. " +
                "\nSkewed mapper Task ID: " + mappers[deviation].getTaskId().toString() +
                "\nAverage mapper input: " + FileUtils.byteCountToDisplaySize(average) +
                "\nSkewed mapper input: " + FileUtils.byteCountToDisplaySize(bytes) + " (" + String.format("%.2fx", (float) bytes / (float) average) + ")";
        return new HeuristicResult(error, false);
    }
}
