package com.linkedin.drelephant.hadoop;

import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskReport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HadoopJobData {
    private HadoopCounterHolder counterHolder;
    private List<HadoopTaskData> mapperData = new ArrayList<HadoopTaskData>();
    private List<HadoopTaskData> reducerData = new ArrayList<HadoopTaskData>();

    public HadoopJobData(RunningJob job, TaskReport[] mapperTasks, TaskReport[] reducerTasks) throws IOException {
        counterHolder = new HadoopCounterHolder(job.getCounters());
        for (TaskReport task : mapperTasks) {
            mapperData.add(new HadoopTaskData(task));
        }
        for (TaskReport task : reducerTasks) {
            reducerData.add(new HadoopTaskData(task));
        }
    }

    public HadoopCounterHolder getCounters() {
        return counterHolder;
    }

    public List<HadoopTaskData> getMapperData() {
        return mapperData;
    }

    public List<HadoopTaskData> getReducerData() {
        return reducerData;
    }
}
