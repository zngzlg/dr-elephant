package com.linkedin.drelephant.analysis.heuristics;

import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.hadoop.HadoopCounterHolder;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;
import com.linkedin.drelephant.math.Statistics;
import org.apache.commons.io.FileUtils;

public class UnsplittableFilesHeuristic implements Heuristic {
    @Override
    public HeuristicResult apply(HadoopJobData data) {
        HadoopTaskData[] mappers = data.getMapperData();

        //Ignore single task jobs
        if (mappers.length <= 1) {
            return HeuristicResult.SUCCESS;
        }

        long[] byte_counters = new long[mappers.length];

        for (int i = 0; i < mappers.length; i++) {
            byte_counters[i] = mappers[i].getCounters().get(HadoopCounterHolder.CounterName.HDFS_BYTES_READ);
        }

        long average = Statistics.average(byte_counters);

        long HDFS_BLOCK_SIZE = Constants.HDFS_BLOCK_SIZE;

        long limit = (long) ((double) HDFS_BLOCK_SIZE * 2D);
        if (average > limit) {
            return buildError(average, HDFS_BLOCK_SIZE);
        }

        return HeuristicResult.SUCCESS;
    }

    private HeuristicResult buildError(long average, long block_size) {
        String error = "Unsplittable files at mapper side." +
                "\nAverage mapper input: " + FileUtils.byteCountToDisplaySize(average) +
                "\nHDFS Block size: " + FileUtils.byteCountToDisplaySize(block_size);
        return new HeuristicResult(error, false);
    }
}
