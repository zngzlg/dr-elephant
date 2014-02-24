package com.linkedin.drelephant;

import com.linkedin.drelephant.hadoop.HadoopJobData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class DrElephant {
    public static void main(String[] args) throws IOException {
        if (args.length > 0) {
            DrElephant elephant = new DrElephant();
            elephant.analyse(args[0]);
        }
    }

    public void analyse(String jobId) throws IOException {
        JobID job_id = JobID.forName(jobId);
        ElephantFetcher fetcher = new ElephantFetcher();
        HadoopJobData jobData = fetcher.getJobData(job_id);
    }
}