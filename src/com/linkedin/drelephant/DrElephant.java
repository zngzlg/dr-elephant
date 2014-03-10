package com.linkedin.drelephant;

import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import org.apache.hadoop.mapred.JobID;

import java.io.File;
import java.io.IOException;

public class DrElephant {
    public static void main(String[] args) throws IOException {
        if (args.length > 0) {
            analyse(args[0]);
        } else {
            File storage = new File("results.txt");
            if (!storage.exists()) {
                storage.createNewFile();
            }

            ElephantRunner elephant = new ElephantRunner(storage);
            elephant.run();
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
}