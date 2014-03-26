package com.linkedin.drelephant.hadoop;

import java.io.IOException;

public class HadoopJobData {
    private String username = "";
    private String url = "";
    private String jobName = "";
    private long startTime = 0;
    private HadoopCounterHolder counterHolder;
    private HadoopTaskData[] mapperData;
    private HadoopTaskData[] reducerData;

    public HadoopJobData(HadoopCounterHolder counters, HadoopTaskData[] mappers, HadoopTaskData[] reducers) throws IOException {
        counterHolder = counters;
        mapperData = mappers;
        reducerData = reducers;
    }

    public HadoopJobData setUsername(String username) {
        this.username = username;
        return this;
    }

    public HadoopJobData setStartTime(long startTime) {
        this.startTime = startTime;
        return this;
    }

    public HadoopJobData setUrl(String url) {
        this.url = url;
        return this;
    }

    public HadoopJobData setJobName(String jobName) {
        this.jobName = jobName;
        return this;
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

    public String getUsername() {
        return username;
    }

    public long getStartTime() {
        return startTime;
    }

    public String getUrl() {
        return url;
    }

    public String getJobName() {
        return jobName;
    }
}
