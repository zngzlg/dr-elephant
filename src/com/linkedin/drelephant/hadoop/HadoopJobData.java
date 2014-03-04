package com.linkedin.drelephant.hadoop;

import com.linkedin.drelephant.ElephantFetcher;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskReport;

import java.io.IOException;

public class HadoopJobData {
    private HadoopCounterHolder counterHolder;
    private HadoopTaskData[] mapperData;
    private HadoopTaskData[] reducerData;

    public HadoopJobData(HadoopCounterHolder counters, HadoopTaskData[] mappers, HadoopTaskData[] reducers) throws IOException {
        counterHolder = counters;
        mapperData = mappers;
        reducerData = reducers;
    }

    public HadoopCounterHolder getCounters() {
        return counterHolder;
    }

    public HadoopTaskData[] getMapperData() {
        return mapperData;
    }

    public HadoopTaskData[] getReducerData() {
        return reducerData;
    }
}
