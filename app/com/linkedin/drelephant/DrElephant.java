package com.linkedin.drelephant;

import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import org.apache.hadoop.mapred.JobID;

import java.io.File;
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
        HeuristicResult result = analyser.analyse(jobData);
        System.out.println(result.getMessage());
    }

    public DrElephant() throws IOException {
        File storage = new File("results.txt");
        if (!storage.exists()) {
            try {
                storage.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        elephant = new ElephantRunner(storage);
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