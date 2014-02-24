package com.linkedin.drelephant;

import com.linkedin.drelephant.hadoop.HadoopJobData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.*;
import org.apache.log4j.Logger;

import java.io.IOException;

public class ElephantFetcher {
    private static final Logger logger = Logger.getLogger(ElephantFetcher.class);
    JobClient jobClient;

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

        return new HadoopJobData(job, mapperTasks, reducerTasks);
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
}
