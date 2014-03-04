package com.linkedin.drelephant;

import com.linkedin.drelephant.hadoop.HadoopCounterHolder;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.*;
import org.apache.log4j.Logger;

import java.io.IOException;

public class ElephantFetcher {
    private static final Logger logger = Logger.getLogger(ElephantFetcher.class);

    private JobClient jobClient;

    public ElephantFetcher() throws IOException {
        init();
    }

    private void init() throws IOException {
        logger.info("Connecting to the jobtracker");
        jobClient = new JobClient(new JobConf(new Configuration()));
    }

    public HadoopJobData getJobData(JobID job_id) throws IOException {
        RunningJob job = getJob(job_id);
        if (job == null) {
            return null;
        }

        TaskReport[] mapperTasks = getMapTaskReports(job_id);
        TaskReport[] reducerTasks = getReduceTaskReports(job_id);

        HadoopCounterHolder counterHolder = new HadoopCounterHolder(job.getCounters());
        HadoopTaskData[] mappers = new HadoopTaskData[mapperTasks.length];
        for (int i = 0; i < mapperTasks.length; i++) {
            mappers[i] = new HadoopTaskData(job, mapperTasks[i], false);
        }
        HadoopTaskData[] reducers = new HadoopTaskData[reducerTasks.length];
        for (int i = 0; i < reducerTasks.length; i++) {
            reducers[i] = new HadoopTaskData(job, reducerTasks[i], true);
        }

        return new HadoopJobData(counterHolder, mappers, reducers);
    }

    private RunningJob getJob(JobID job_id) throws IOException {
        logger.info("Fetching job " + job_id);
        return jobClient.getJob(job_id);
    }

    private TaskReport[] getMapTaskReports(JobID job_id) throws IOException {
        logger.info("Fetching mapper data for job " + job_id);
        return jobClient.getMapTaskReports(job_id);
    }

    private TaskReport[] getReduceTaskReports(JobID job_id) throws IOException {
        logger.info("Fetching reducer data for job " + job_id);
        return jobClient.getReduceTaskReports(job_id);
    }

    public JobStatus[] getJobList() throws IOException {
        logger.info("Fetching job list");
        return jobClient.getAllJobs();
    }
}
