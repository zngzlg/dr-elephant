package com.linkedin.drelephant;

import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import org.apache.hadoop.mapred.JobID;

import java.io.IOException;

public class DrElephant extends Thread {
    private ElephantRunner elephant;

    public static void main(String[] args) throws IOException {
        if (args.length > 0) {
            analyse(args[0]);
        } else {
            new DrElephant().start();
        }
    }

    public static void analyse(String jobId) throws IOException {
        JobID job_id = JobID.forName(jobId);
        ElephantFetcher fetcher = new ElephantFetcher();
        HadoopJobData jobData = fetcher.getJobData(job_id);
        ElephantAnalyser analyser = new ElephantAnalyser();
        HeuristicResult[] result = analyser.analyse(jobData);
    }

    public DrElephant() throws IOException {
        elephant = new ElephantRunner();
    }

    @Override
    public void run() {
        elephant.run();
    }

    public void kill() {
        if (elephant != null) {
            elephant.kill();
        }
    }
}