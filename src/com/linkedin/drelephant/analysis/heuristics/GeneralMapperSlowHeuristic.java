package com.linkedin.drelephant.analysis.heuristics;

import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.hadoop.HadoopCounterHolder;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;
import com.linkedin.drelephant.math.Statistics;
import org.apache.commons.io.FileUtils;

public class GeneralMapperSlowHeuristic implements Heuristic {
    @Override
    public HeuristicResult apply(HadoopJobData data) {
        HadoopTaskData[] mappers = data.getMapperData();

        long[] speeds = new long[mappers.length];
        long[] runtime = new long[mappers.length];

        for (int i = 0; i < mappers.length; i++) {
            long input_bytes = mappers[i].getCounters().get(HadoopCounterHolder.CounterName.HDFS_BYTES_READ);
            runtime[i] = mappers[i].getEndTime() - mappers[i].getStartTime();
            //Apply 1 minute buffer
            runtime[i] -= 60 * 1000;
            if (runtime[i] < 1000) {
                runtime[i] = 1000;
            }
            //Speed is bytes per second
            speeds[i] = (1000 * input_bytes) / (runtime[i]);
        }

        long average = Statistics.average(speeds);
        long avgRuntime = Statistics.average(runtime);

        if (avgRuntime < 10 * 60 * 1000) {
            // Less than 10 minutes
            return HeuristicResult.SUCCESS;
        }

        long diskReadSpeed = Constants.DISK_READ_SPEED;
        long limit = diskReadSpeed / 4;

        if (average < limit) {
            return buildError(average);
        }

        return HeuristicResult.SUCCESS;
    }

    private HeuristicResult buildError(long average) {
        String error = "Mapper side is unusually slow." + "" +
                "\nAverage mapper speed: " + FileUtils.byteCountToDisplaySize(average) + "/s";
        return new HeuristicResult(error, false);
    }
}
